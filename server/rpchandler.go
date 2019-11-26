package server

import (
	"google.golang.org/grpc/status"
	"sync"
	"sync/atomic"

	pb "github.com/xorlev/slogd/proto"
	"github.com/xorlev/slogd/storage"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

type StructuredLogServer struct {
	clientID uint64

	sync.RWMutex

	config *Config
	logger *zap.SugaredLogger
	// map from string to Log
	topics      map[string]storage.Log
	subscribers map[string]map[uint64]storage.LogEntryChannel
}

func (s *StructuredLogServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	if !isValidTopic(req.GetTopic()) {
		return nil, status.Errorf(codes.InvalidArgument, "Topic must conform to pattern [a-z0-9_\\-.]+")
	}

	resp := &pb.GetLogsResponse{}
	s.logger.Debugw("Processing GetLogs call",
		"request", req,
	)

	s.RLock()
	topic, ok := s.topics[req.GetTopic()]
	s.RUnlock()

	if ok {
		filter, err := storage.NewLogQuery(req)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Failed to create query: %+v", err)
		}

		s.logger.Debugf("Filter = %+v", filter)

		logRetrieval, err := topic.Retrieve(ctx, filter)
		if err != nil {
			s.logger.Errorw("Failed to retrieve logs",
				"req", req,
				"err", err,
			)
			return nil, status.Errorf(codes.Internal, "Error retrieving logs: %v", err)
		}

		resp.Logs = logRetrieval.Logs

		return resp, nil
	} else {
		return nil, status.Errorf(codes.NotFound, "Topic does not exist: %v", req.GetTopic())
	}
}

func (s *StructuredLogServer) AppendLogs(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	if !isValidTopic(req.GetTopic()) {
		return nil, status.Errorf(codes.InvalidArgument, "Topic must conform to pattern [a-z0-9_\\-.]+")
	}

	resp := &pb.AppendResponse{}

	s.logger.Debugw("Processing AppendLogs call",
		"request", req,
	)

	s.Lock()
	topic, ok := s.topics[req.GetTopic()]

	// Auto-create new topic
	if !ok {
		log, err := storage.NewFileLog(s.logger, s.config.DataDir, req.GetTopic())

		if err != nil {
			s.Unlock()
			return nil, status.Errorf(codes.Internal, "Error creating new topic: %v", err)
		}

		go s.pubsubListener(log)
		s.topics[req.GetTopic()] = log
		topic = log
	}
	s.Unlock()

	offsets, err := topic.Append(ctx, req.GetLogs())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error appending logs: %v", err)
	}

	s.logger.Debug("Post-append")

	resp.Offsets = offsets

	return resp, nil
}

func (s *StructuredLogServer) GetLogsStream(req *pb.GetLogsRequest, stream pb.StructuredLog_GetLogsStreamServer) error {
	if !isValidTopic(req.GetTopic()) {
		return status.Errorf(codes.InvalidArgument, "Topic must conform to pattern [a-z0-9_\\-.]+")
	}

	clientID := atomic.AddUint64(&s.clientID, 1)

	s.logger.Debugw("Processing StreamLogs call",
		"request", req,
	)

	s.RLock()
	topic, ok := s.topics[req.GetTopic()]
	s.RUnlock()

	if ok {
		ch := s.addSubscriber(req, clientID)

		filter, err := storage.NewLogQuery(req)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Failed to create query: %+v", err)
		}

		s.logger.Debugf("Filter = %+v", filter)

		c := storage.NewCursor(stream.Context(), filter, ch, 1000)

		if err := c.Consume(topic, func(log *pb.LogEntry) error { return stream.SendMsg(log) }); err != nil {
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
				if err := c.Consume(topic, func(log *pb.LogEntry) error { return stream.SendMsg(log) }); err != nil {
					return err
				}
			}
		}
	} else {
		return status.Errorf(codes.InvalidArgument, "Topic does not exist: %v", req.GetTopic())
	}
}

func (s *StructuredLogServer) StreamLogs(req *pb.GetLogsRequest, stream pb.StructuredLog_StreamLogsServer) error {
	if !isValidTopic(req.GetTopic()) {
		return status.Errorf(codes.InvalidArgument, "Topic must conform to pattern [a-z0-9_\\-.]+")
	}

	clientID := atomic.AddUint64(&s.clientID, 1)

	s.logger.Debugw("Processing StreamLogs call",
		"request", req,
	)

	s.RLock()
	topic, ok := s.topics[req.GetTopic()]
	s.RUnlock()

	if ok {
		ch := s.addSubscriber(req, clientID)

		filter, err := storage.NewLogQuery(req)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Failed to create query: %+v", err)
		}

		s.logger.Debugf("Filter = %+v", filter)

		c := storage.NewCursor(stream.Context(), filter, ch, 1000)

		if err := c.Consume(topic, func(log *pb.LogEntry) error {
			return stream.SendMsg(logToResponse(log))
		}); err != nil {
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
				if err := c.Consume(topic, func(log *pb.LogEntry) error {
					return stream.SendMsg(logToResponse(log))
				}); err != nil {
					return err
				}
			}
		}
	} else {
		return status.Errorf(codes.InvalidArgument, "Topic does not exist: %v", req.GetTopic())
	}
}

func (s *StructuredLogServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	resp := &pb.ListTopicsResponse{}

	s.RLock()
	for topic, _ := range s.topics {
		resp.Topic = append(resp.Topic, &pb.Topic{Name: topic})
	}
	s.RUnlock()

	return resp, nil
}

func (s *StructuredLogServer) Close() error {
	s.Lock()
	defer s.Unlock()

	for _, log := range s.topics {
		log.Close()
	}

	for _, subscribers := range s.subscribers {
		for _, channel := range subscribers {
			close(channel)
		}
	}

	return nil
}

// Reads fanin channel, identifies interested subscribers (StreamLogs clients)
// and notifies them that new logs are available for consumptuon
func (s *StructuredLogServer) pubsubListener(log storage.Log) {
	s.logger.Info("Starting pubsub listener..")
	for {
		for log := range log.LogChannel() {
			s.logger.Debugf("Received log: %+v", log)

			s.RLock()
			val, ok := s.subscribers[log.Topic]
			if ok {
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
	defer s.Unlock()
	close(s.subscribers[req.GetTopic()][clientID])
	delete(s.subscribers[req.GetTopic()], clientID)
}

// Convert a single log entry to a full GetLogsResponse
func logToResponse(log *pb.LogEntry) *pb.GetLogsResponse {
	return &pb.GetLogsResponse{
		Logs: []*pb.LogEntry{log},
	}
}

func NewRpcHandler(logger *zap.SugaredLogger, config *Config) (*StructuredLogServer, error) {
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

	for _, log := range topics {
		go s.pubsubListener(log)
	}

	return s, nil
}
