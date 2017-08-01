package storage

import (
	pb "github.com/xorlev/slogd/proto"
)

// Convert a single log entry to a full GetLogsResponse
func logToResponse(log *pb.LogEntry) *pb.GetLogsResponse {
	return &pb.GetLogsResponse{
		Logs: []*pb.LogEntry{log},
	}
}
