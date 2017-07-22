package server

import (
	"github.com/gogo/protobuf/types"
	pb "github.com/xorlev/slogd/proto"
	storage "github.com/xorlev/slogd/storage"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"sync"
	"sync/atomic"
	"time"
)

type StructuredLogServer struct {
	clientID uint64

	sync.RWMutex

	config *Config
	logger *zap.SugaredLogger
	// map from string to Log
	topics      map[string]storage.Log
	subscribers map[string]map[uint64]storage.LogEntryChannel

	fanin storage.LogEntryChannel
}

func (s *StructuredLogServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	resp := &pb.GetLogsResponse{}
	s.logger.Debugw("Processing GetLogs call",
		"request", req,
	)

	s.RLock()
	topic, ok := s.topics[req.GetTopic()]
	s.RUnlock()

	if ok {
		var t time.Time = time.Time{}

		if req.GetTimestamp() != nil {
			var err error = nil
			t, err = types.TimestampFromProto(req.GetTimestamp())
			if err != nil {
				return nil, grpc.Errorf(codes.InvalidArgument, "Bad timestamp: %+v", err)
			}
		}

		filter := &storage.LogFilter{
			StartOffset: req.GetOffset(),
			MaxMessages: uint32(req.GetMaxMessages()),
			Timestamp:   t,
		}

		logs, _, err := topic.Retrieve(ctx, filter)
		if err != nil {
			s.logger.Errorw("Failed to retrieve logs",
				"req", req,
				"err", err,
			)
			return nil, grpc.Errorf(codes.Internal, "Error retrieving logs: %v", err)
		}

		resp.Logs = logs

		return resp, nil
	} else {
		return nil, grpc.Errorf(codes.InvalidArgument, "Topic does not exist: %v", req.GetTopic())
	}
}

func (s *StructuredLogServer) AppendLogs(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	if !isValidTopic(req.GetTopic()) {
		return nil, grpc.Errorf(codes.InvalidArgument, "Topic must conform to pattern [a-z0-9_\\-.]+")
	}

	resp := &pb.AppendResponse{}

	s.logger.Debugw("Processing AppendLogs call",
		"request", req,
	)

	s.Lock()
	topic, ok := s.topics[req.GetTopic()]
	if !ok {
		// TODO fix subscriber agg channel
		log, err := storage.NewFileLog(s.logger, s.config.DataDir, req.GetTopic())

		if err != nil {
			s.Unlock()
			return nil, grpc.Errorf(codes.Internal, "Error creating new topic: %v", err)
		}

		s.startTopicWatcher(log)
		s.topics[req.GetTopic()] = log
		topic = log
	}
	s.Unlock()

	offsets, err := topic.Append(ctx, req.GetLogs())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Error appending logs: %v", err)
	}

	s.logger.Debug("Post-append")

	resp.Offsets = offsets

	return resp, nil
}

func (s *StructuredLogServer) StreamLogs(req *pb.GetLogsRequest, stream pb.StructuredLog_StreamLogsServer) error {
	clientID := atomic.AddUint64(&s.clientID, 1)

	s.logger.Debugw("Processing StreamLogs call",
		"request", req,
	)

	s.RLock()
	topic, ok := s.topics[req.GetTopic()]
	s.RUnlock()

	if ok {

		ch := s.addSubscriber(req, clientID)

		c := &cursor{
			ctx:            stream.Context(),
			lastOffset:     min(req.GetOffset(), topic.Length()),
			unreadMessages: ch,
		}

		if err := c.consume(topic, stream.SendMsg); err != nil {
			return err
		}
		for {
			select {
			case <-stream.Context().Done():
				s.logger.Infow("Stream finished.",
					"clientID", clientID,
				)

				s.removeSubscriber(req, clientID)

				// Done, remove
				return nil
			case <-ch:
				if err := c.consume(topic, stream.SendMsg); err != nil {
					return err
				}
			}
		}

		select {
		case <-ch:
			// ignore, we're just clearing the channel if it has a value
		default:

		}
	} else {
		return grpc.Errorf(codes.InvalidArgument, "Topic does not exist: %v", req.GetTopic())
	}

	return nil
}

// really, go?
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (s *StructuredLogServer) addSubscriber(req *pb.GetLogsRequest, clientID uint64) storage.LogEntryChannel {
	s.Lock()
	val, ok := s.subscribers[req.GetTopic()]

	ch := make(storage.LogEntryChannel)
	if ok {
		val[clientID] = ch
	} else {
		s.subscribers[req.GetTopic()] = make(map[uint64]storage.LogEntryChannel)
		s.subscribers[req.GetTopic()][clientID] = ch
	}
	s.Unlock()

	return ch
}

func (s *StructuredLogServer) removeSubscriber(req *pb.GetLogsRequest, clientID uint64) {
	s.Lock()
	close(s.subscribers[req.GetTopic()][clientID])
	delete(s.subscribers[req.GetTopic()], clientID)
	s.Unlock()
}

func (s *StructuredLogServer) pubsubStub(logChannel storage.LogEntryChannel) {
	s.logger.Info("Starting pubsub listener..")
	for {
		for log := range logChannel {
			s.logger.Debugf("Published log: %+v", log)

			s.RLock()
			val, ok := s.subscribers[log.Topic]
			if ok {
				// non-blocking w/ select?
				for _, subscriber := range val {
					select {
					case subscriber <- log:
						// Noop
					default:
						// Skip
					}
				}
			}
			s.RUnlock()
		}
	}
}

func NewRpcHandler(logger *zap.SugaredLogger, config *Config) (*StructuredLogServer, error) {
	// Read all topics (folder in storage dir)
	// Spin off Goroutine for each topic
	// Start server, protect with RWLock

	logger.Infow("Data dir",
		"data_dir", config.DataDir,
	)

	topics, err := storage.OpenLogs(logger, config.DataDir)
	if err != nil {
		return nil, err
	}

	s := &StructuredLogServer{
		config:      config,
		logger:      logger,
		topics:      topics,
		subscribers: make(map[string]map[uint64]storage.LogEntryChannel),
	}

	agg := make(storage.LogEntryChannel)
	for _, log := range topics {
		s.startTopicWatcher(log)
	}

	go s.pubsubStub(agg)

	return s, nil
}

func (s *StructuredLogServer) startTopicWatcher(log storage.Log) {
	go func(c storage.LogEntryChannel) {
		for msg := range c {
			s.fanin <- msg
		}
	}(log.LogChannel())
}

func (s *StructuredLogServer) Close() error {
	s.Lock()
	defer s.Unlock()

	for _, log := range s.topics {
		log.Close()
	}

	return nil
}
