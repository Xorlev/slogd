package server

import (
	pb "github.com/xorlev/slogd/proto"
	storage "github.com/xorlev/slogd/storage"
	"golang.org/x/net/context"
)

type cursor struct {
	ctx            context.Context
	lastOffset     uint64
	unreadMessages storage.LogEntryChannel
}

func (c *cursor) consume(topic storage.Log, f func(interface{}) error) error {
	for {
		// read pages, go to sleep until dirty bit is set, read pages
		filter := &storage.LogFilter{
			StartOffset: c.lastOffset + 1,
			MaxMessages: 1000,
		}

		initial, _, err := topic.Retrieve(c.ctx, filter)
		if err != nil {
			return err
		}

		lastOffset := c.lastOffset
		for _, log := range initial {
			lastOffset = log.GetOffset()
			if err := f(toResponse(log)); err != nil {
				return err
			}
		}

		if len(initial) == 0 || lastOffset == c.lastOffset {
			return nil
		}

		c.lastOffset = lastOffset
	}

	return nil
}

func toResponse(log *pb.LogEntry) *pb.GetLogsResponse {
	return &pb.GetLogsResponse{
		Logs: []*pb.LogEntry{log},
	}
}
