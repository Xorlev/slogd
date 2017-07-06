package server

import (
	"go.uber.org/zap"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "github.com/xorlev/slogd/proto"
	storage "github.com/xorlev/slogd/storage"
)

type structuredLogServer struct {
	config *Config
	logger *zap.SugaredLogger
	rwLock sync.RWMutex
	// map from string to Log
	topics map[string]storage.Log
}

func (s *structuredLogServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	resp := &pb.GetLogsResponse{}
	s.logger.Debugw("Processing GetLogs call",
		"request", req,
	)

	s.rwLock.RLock()
	val, ok := s.topics[req.GetTopic()]
	s.rwLock.RUnlock()

	if ok {
		logs, err := val.Retrieve(req)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "Error retrieving logs: %v", err)
		}

		resp.Logs = logs

		return resp, nil
	} else {
		return nil, grpc.Errorf(codes.InvalidArgument, "Topic does not exist: %v", req.GetTopic())
	}
}

func (s *structuredLogServer) AppendLogs(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	resp := &pb.AppendResponse{}

	s.logger.Debugw("Processing AppendLogs call",
		"request", req,
	)

	s.rwLock.Lock()
	val, ok := s.topics[req.GetTopic()]
	if !ok {
		log, err := storage.NewFileLog(s.logger, ".", req.GetTopic())

		if err != nil {
			s.rwLock.Unlock()
			return nil, grpc.Errorf(codes.Internal, "Error creating new topic: %v", err)
		}

		s.topics[req.GetTopic()] = log
		val = log
	}
	s.rwLock.Unlock()

	offsets, err := val.Append(req.GetLogs())
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "Error appending logs: %v", err)
	}

	s.logger.Debug("Post-append")

	resp.Status = pb.Status_OK
	resp.Offsets = offsets

	return resp, nil
}

func (s *structuredLogServer) StreamLogs(req *pb.GetLogsRequest, stream pb.StructuredLog_StreamLogsServer) error {
	s.logger.Debugw("Processing StreamLogs call",
		"request", req,
	)

	// initial GetLogs to replay to now(), then stream
	return nil
}

func pubsubStub(logger *zap.SugaredLogger, logChannel storage.LogEntryChannel) {
	logger.Info("Starting pubsub listener..")
	for {
		for log := range logChannel {
			logger.Infof("Published log: %+v", log)
		}
	}
}

func NewRpcHandler(logger *zap.SugaredLogger, config *Config) (*structuredLogServer, error) {
	// Read all topics (folder in storage dir)
	// Spin off Goroutine for each topic
	// Start server, protect with RWLock

	logger.Infow("Data dir",
		"data_dir", config.DataDir,
	)

	// log, err := storage.NewFileLog(logger, config.DataDir, "crawls")
	// if err != nil {
	// 	return nil, err
	// }

	// s.topics["crawls"] = log

	topics, err := storage.OpenLogs(logger, config.DataDir)
	if err != nil {
		return nil, err
	}

	s := &structuredLogServer{
		config: config,
		logger: logger,
		topics: topics,
	}

	go pubsubStub(logger, s.topics["crawls"].LogChannel())

	return s, nil
}
