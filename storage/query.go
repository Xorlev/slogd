package storage

import (
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/xorlev/slogd/proto"
	"time"
)

type LogQuery struct {
	MaxMessages    uint32
	Position       position
	StartOffset    uint64
	Timestamp      time.Time
	cursorPosition cursorPosition
}

func NewLogQuery(req *proto.GetLogsRequest) (*LogQuery, error) {
	filter := &LogQuery{
		MaxMessages: uint32(req.GetMaxMessages()),
		Position: EXACT,
	}

	switch req.StartAt.(type) {
	case *proto.GetLogsRequest_Timestamp:
		t, err := types.TimestampFromProto(req.GetTimestamp())
		if err != nil {
			return nil, errors.Errorf("Bad timestamp: %v", req.GetTimestamp())
		}
		filter.Timestamp = t
	case *proto.GetLogsRequest_Offset:
		filter.StartOffset = req.GetOffset()
	case *proto.GetLogsRequest_Position_:
		switch req.GetPosition() {
		case proto.GetLogsRequest_LATEST:
			filter.Position = LATEST
		case proto.GetLogsRequest_EARLIEST:
			filter.Position = EARLIEST
		}
	case nil:
	default:
	}

	return filter, nil
}

func (query *LogQuery) LogPassesFilter(entry *proto.LogEntry) (bool, error) {
	if !query.Timestamp.IsZero() {
		ts, err := types.TimestampFromProto(entry.GetTimestamp())
		if err != nil {
			return false, err
		}

		return ts == query.Timestamp || ts.After(query.Timestamp), nil
	} else if query.StartOffset > 0 {
		return entry.GetOffset() >= query.StartOffset, nil
	}

	return true, nil
}

func (query *LogQuery) SegmentPassesFilter(segment logSegment) (bool, error) {
	offsetMatches := segment.EndOffset() >= query.StartOffset
	timestampMatches := !segment.StartTime().IsZero() &&
		!segment.EndTime().IsZero() &&
		!query.Timestamp.IsZero() &&
		segment.EndTime().After(query.Timestamp)

	return offsetMatches || timestampMatches, nil
}

type LogRetrieval struct {
	Logs     []*proto.LogEntry
	position cursorPosition
}

