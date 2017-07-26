package server

import (
	storage "github.com/xorlev/slogd/storage"
	"golang.org/x/net/context"
)

type cursor struct {
	ctx            context.Context
	lastOffset     uint64
	unreadMessages storage.LogEntryChannel
}

func (c *cursor) consume(topic storage.Log, f func(interface{}) error) error {
	// Page through log until no more messages show up, then return control to caller
	for {
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
			if err := f(logToResponse(log)); err != nil {
				return err
			}
		}

		// If we didn't get any new logs, we're done
		if len(initial) == 0 || lastOffset == c.lastOffset {
			return nil
		}

		c.lastOffset = lastOffset
	}

	return nil
}
