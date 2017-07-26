package server

import (
	pb "github.com/xorlev/slogd/proto"
	"regexp"
)

var topicValid = regexp.MustCompile(`^[a-z0-9_\-.]+$`)

func isValidTopic(topic string) bool {
	return topicValid.MatchString(topic)
}

// really, go?
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// Convert a single log entry to a full GetLogsResponse
func logToResponse(log *pb.LogEntry) *pb.GetLogsResponse {
	return &pb.GetLogsResponse{
		Logs: []*pb.LogEntry{log},
	}
}
