package internal

import (
	pb "github.com/xorlev/slogd/proto"
)

type TopicAndLog struct {
	Topic    string
	LogEntry *pb.LogEntry
}
