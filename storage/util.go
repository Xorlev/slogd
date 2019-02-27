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

// Really, Go?
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}