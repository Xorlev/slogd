package server

import (
	storage "github.com/xorlev/slogd/storage"
	"golang.org/x/net/context"
)

type cursor struct {
	ctx            context.Context
	continuation   storage.Continuation
	logQuery       storage.LogQuery
	unreadMessages storage.LogEntryChannel
}

func (c *cursor) consume(topic storage.Log, f func(interface{}) error) error {
	// Page through log until no more messages show up, then return control to caller
	for {
		var logQuery *storage.LogQuery = nil
		if c.continuation.FilePosition < 0 {
			logQuery = &c.logQuery
			logQuery.MaxMessages = 1000
		} else {
			logQuery = &storage.LogQuery{
				StartOffset: c.continuation.LastOffsetRead + 1,
				MaxMessages: 1000,
			}
		}

		initial, err := topic.Retrieve(c.ctx, logQuery, &c.continuation)
		if err != nil {
			return err
		}

		lastOffset := c.continuation.LastOffsetRead
		for _, log := range initial.Logs {
			lastOffset = log.GetOffset()
			if err := f(logToResponse(log)); err != nil {
				return err
			}
		}

		// If we didn't get any new logs, we're done
		if len(initial.Logs) == 0 || lastOffset == c.continuation.LastOffsetRead {
			return nil
		}

		c.continuation = initial.Continuation
	}

	return nil
}

func newCursor(ctx context.Context, lf *storage.LogQuery, logChannel storage.LogEntryChannel) *cursor {
	return &cursor{
		ctx: ctx,
		continuation: storage.Continuation{
			FilePosition: -1,
		},
		logQuery:       *lf,
		unreadMessages: logChannel,
	}
}
