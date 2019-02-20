package storage

import (
	"golang.org/x/net/context"
)

// Cursor supports efficient iteration through a Log, calling a callback for
// each message returned. Cursors maintain state about the underlying file
// positions allowing indexless reads over large chunks of logfile.
type Cursor struct {
	ctx            context.Context
	position       cursorPosition
	logsPerRequest uint32
	query          LogQuery
	unreadMessages LogEntryChannel
}

// Consume will consume from the given log, invoking f for each message matching
// the provided LogQuery. Upon finding the end of the log (or an error),
// Consume will return nil or the error. The caller must call Consume again to
// retrieve logs since the last invocation returned.
func (c *Cursor) Consume(topic Log, f func(interface{}) error) error {
	// Page through log until no more messages show up, then return control to caller
	for {
		var logQuery *LogQuery
		if c.position.FilePosition < 0 {
			logQuery = &c.query
			logQuery.MaxMessages = c.logsPerRequest
		} else {
			logQuery = &LogQuery{
				StartOffset: c.position.LastOffsetRead + 1,
				MaxMessages: c.logsPerRequest,
			}
		}

		read, err := topic.retrieve(c.ctx, logQuery, &c.position)
		if err != nil {
			return err
		}

		lastOffset := c.position.LastOffsetRead
		for _, log := range read.Logs {
			lastOffset = log.GetOffset()
			if err := f(logToResponse(log)); err != nil {
				return err
			}
		}

		// If we didn't get any new logs, we're done
		if len(read.Logs) == 0 || lastOffset == c.position.LastOffsetRead {
			return nil
		}

		c.position = read.position
	}
}

// NewCursor creates a new cursor.
func NewCursor(ctx context.Context, query *LogQuery, logChannel LogEntryChannel, logsPerRequest uint32) *Cursor {
	return &Cursor{
		ctx: ctx,
		// FilePos -1 signals that the LogQuery is to be used to start
		position: cursorPosition{
			FilePosition: -1,
		},
		query:          *query,
		unreadMessages: logChannel,
		logsPerRequest: logsPerRequest,
	}
}
