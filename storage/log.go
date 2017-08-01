package storage

import (
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/xorlev/slogd/internal"
	pb "github.com/xorlev/slogd/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type LogEntryChannel chan *internal.TopicAndLog

type Log interface {
	Name() string
	LogChannel() LogEntryChannel
	Retrieve(context.Context, *LogQuery, *Continuation) (*LogRetrieval, error)
	Append(context.Context, []*pb.LogEntry) ([]uint64, error)
	Length() uint64
	Close() error
}

type FileLog struct {
	Log
	sync.RWMutex

	config              *pb.TopicConfig
	logger              *zap.SugaredLogger
	basePath            string
	closed              bool
	topic               string
	messagesWithOffsets LogEntryChannel
	nextOffset          uint64
	segments            []logSegment
	currentSegment      logSegment
	closeCh             chan bool
}

type Continuation struct {
	LastOffsetRead uint64
	FilePosition   int64
}

type position int

const (
	LATEST position = iota
	EARLIEST
)

type LogQuery struct {
	MaxMessages  uint32
	Position     position
	StartOffset  uint64
	Timestamp    time.Time
	Continuation Continuation
}

func NewLogQuery(req *pb.GetLogsRequest) (*LogQuery, error) {
	filter := &LogQuery{
		MaxMessages: uint32(req.GetMaxMessages()),
	}

	switch req.StartAt.(type) {
	case *pb.GetLogsRequest_Timestamp:
		t, err := types.TimestampFromProto(req.GetTimestamp())
		if err != nil {
			return nil, errors.Errorf("Bad timestamp: %v", req.GetTimestamp())
		}
		filter.Timestamp = t
	case *pb.GetLogsRequest_Offset:
		filter.StartOffset = req.GetOffset()
	case *pb.GetLogsRequest_Position_:
		switch req.GetPosition() {
		case pb.GetLogsRequest_LATEST:
			filter.Position = LATEST
		case pb.GetLogsRequest_EARLIEST:
			filter.Position = EARLIEST
		}
	case nil:
	default:
	}

	return filter, nil
}

func (query *LogQuery) LogPassesFilter(entry *pb.LogEntry) (bool, error) {
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

type LogRetrieval struct {
	Logs         []*pb.LogEntry
	Continuation Continuation
}

func (query *LogQuery) SegmentPassesFilter(segment logSegment) (bool, error) {
	offsetMatches := segment.EndOffset() >= query.StartOffset
	timestampMatches := !segment.StartTime().IsZero() && !segment.EndTime().IsZero() && !query.Timestamp.IsZero() && segment.EndTime().After(query.Timestamp)

	return offsetMatches || timestampMatches, nil
}

func (fl *FileLog) Name() string {
	return fl.topic
}

func (fl *FileLog) LogChannel() LogEntryChannel {
	return fl.messagesWithOffsets
}

func (fl *FileLog) Retrieve(ctx context.Context, query *LogQuery, continuation *Continuation) (*LogRetrieval, error) {
	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0

	fl.RLock()
	segments := make([]logSegment, len(fl.segments))
	copy(segments, fl.segments)
	fl.RUnlock()

	// TODO: race between logfile reaper and segment retrieval
	// Find the covering set of segments with messages that fit our LogFilter, then ask for messages up to
	// the number of messages requested. May span multiple segments.
	var lastOffset uint64 = 0
	var positionEnd int64 = -1
	var lastSegment int = -1
	for i, segment := range segments {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		messagesRead := uint32(len(logEntries))

		// TODO: add EndTimestamp?
		// Evaluate LogFilter against segment
		segmentMatches, err := query.SegmentPassesFilter(segment)
		// fl.logger.Debugf("Segment matches filter: %+v, %+v", lf, segment)
		if err != nil {
			return nil, err
		}

		if segmentMatches && messagesRead < query.MaxMessages {
			var filePosition int64 = -1
			if continuation != nil {
				filePosition = continuation.FilePosition
			}

			segmentLogs, scanned, segmentPositionEnd, err := segment.Retrieve(ctx, query, filePosition, query.MaxMessages-messagesRead)
			if err != nil {
				return nil, errors.Wrap(err, "Error reading from segment")
			}

			logEntries = append(logEntries, segmentLogs...)

			if len(segmentLogs) > 0 {
				lastOffset = segmentLogs[len(segmentLogs)-1].Offset

				// TODO clone
				query.StartOffset = lastOffset + 1
			}

			scannedLogs += scanned
			positionEnd = segmentPositionEnd
			lastSegment = i
			//
			// return nil, errors.New("TODO: Fix segment bounds")
		}
	}

	fl.logger.Infof("Scanned %d logs for %d returned messages",
		scannedLogs, len(logEntries))

	continuationPosition := positionEnd

	// If we're at the end of the segment but not the end of all segments, reset file position
	// TODO: this is pretty nasty. Should have explicit signaling of "finished" segments vs. "in-progress"
	// segments
	if lastSegment >= 0 && lastOffset == segments[lastSegment].EndOffset() && len(segments)-1 > lastSegment {
		continuationPosition = -1
	}

	return &LogRetrieval{
		Logs: logEntries,
		Continuation: Continuation{
			FilePosition:   continuationPosition,
			LastOffsetRead: lastOffset,
		},
	}, nil
}

func (fl *FileLog) Append(ctx context.Context, logs []*pb.LogEntry) ([]uint64, error) {
	fl.Lock()
	defer fl.Unlock()

	// Append each log individually, assign offsets to each
	newOffsets := make([]uint64, len(logs))
	for i, log := range logs {
		if log != nil {
			fl.logger.Debugf("Appending: %+v", log)
			log.Offset = fl.nextOffset
			newOffsets[i] = fl.nextOffset
			fl.nextOffset += 1

			fl.currentSegment.Append(ctx, log)
		}
	}

	// Flush the file for durability
	// TODO: setting to allow flush on each append
	fl.currentSegment.Flush()

	// Roll log segment if necessary
	if err := fl.rollLogIfNecessary(); err != nil {
		return nil, errors.Wrap(err, "Error rolling log")
	}

	// Publish to registered stream
	for _, log := range logs {
		fl.messagesWithOffsets <- &internal.TopicAndLog{
			Topic:    fl.topic,
			LogEntry: log,
		}
	}

	fl.logger.Debug("Done with Append()")

	return newOffsets, nil
}

func (fl *FileLog) Length() uint64 {
	fl.RLock()
	defer fl.RUnlock()

	return fl.nextOffset
}

func (fl *FileLog) Close() error {
	fl.Lock()
	defer fl.Unlock()

	for _, segment := range fl.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	fl.closed = true
	fl.closeCh <- true
	close(fl.messagesWithOffsets)

	return nil
}

func (fl *FileLog) rollLogIfNecessary() error {
	// Called from write lock

	rotateSegmentAfterSeconds := time.Duration(fl.config.RotateSegmentAfterSeconds) * time.Second
	segmentAgeHorizon := time.Now().Add(-rotateSegmentAfterSeconds)

	// If segment is too big or was created too long ago, roll the log
	if fl.currentSegment.SizeBytes() >= fl.config.SegmentSizeLimit || fl.currentSegment.StartTime().Before(segmentAgeHorizon) {
		if err := fl.rollLog(); err != nil {
			return err
		}
	}

	return nil
}

func (fl *FileLog) rollLog() error {
	// If current log is empty, skip
	if fl.currentSegment.SizeBytes() == 0 {
		return nil
	}

	// Open a new log segment
	newSegment, err := openSegment(fl.logger, fl.config, fl.basePath, fl.nextOffset)
	if err != nil {
		return err
	}
	fl.currentSegment = newSegment

	fl.segments = append(fl.segments, fl.currentSegment)

	return nil
}

func (fl *FileLog) reapSegments() error {
	// Check if any stale segments need reaping
	fl.RLock()
	var index = -1
	{
		staleSegmentAge := time.Duration(fl.config.StaleSegmentSeconds) * time.Second
		horizon := time.Now().Add(-staleSegmentAge)
		for i, segment := range fl.segments {
			if segment.EndTime().Before(horizon) && segment.SizeBytes() > 0 {
				fl.logger.Debugf("Found stale segment to remove: %v", segment)
				index = i
			} else {
				break
			}
		}
	}
	fl.RUnlock()

	// Found segments to reap
	if index >= 0 {
		fl.Lock()
		defer fl.Unlock()
		for i := 0; i <= index; i++ {
			// If we only have one segment (or will have zero segments after reaping), roll the log first
			if len(fl.segments) == 1 || index == len(fl.segments)-1 {
				fl.logger.Debugf("Only have %d segments, want to remove %d. Rolling log.", len(fl.segments), index+1)
				fl.rollLog()
			}

			// If we still have one segment, it's empty. Nothing to do.
			if len(fl.segments) == 1 {
				fl.logger.Debugf("Only have a single empty segment, skipping reap.")
				return nil
			}

			// Delete all the segments
			if err := fl.segments[i].Delete(); err != nil {
				return errors.Wrap(err, "Error reaping segment")
			}

			// Allow segment to be GC'd
			fl.segments[i] = nil
		}

		fl.segments = fl.segments[index+1:]
	}

	return nil
}

func (fl *FileLog) logMaintenance() {
	// Periodic maintenance of logs, removing older segments beyond the retention horizon
	ticker := time.NewTicker(time.Duration(fl.config.LogMaintenancePeriod) * time.Second)
	for {
		select {
		case <-ticker.C:
			fl.logger.Info("Periodic maintenance starting.")

			// Check if old segments need to be removed
			if err := fl.reapSegments(); err != nil {
				fl.logger.Errorf("Error reaping segments: %v", err)
			}

			// Check if the current log needs to be rolled (segment too old)
			if err := fl.rollLogIfNecessary(); err != nil {
				fl.logger.Errorf("Error rolling logs: %v", err)
			}

		case <-fl.closeCh:
			fl.logger.Info("Stopping periodic maintenance.")
			ticker.Stop()
			break
		}
	}
}

func OpenLogs(logger *zap.SugaredLogger, directory string) (map[string]Log, error) {
	entries, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	logs := make(map[string]Log)

	for _, entry := range entries {
		if entry.IsDir() {
			// todo validate topic name
			topic := entry.Name()

			logs[topic], err = NewFileLog(logger, directory, topic, DefaultConfig())
			if err != nil {
				return nil, errors.Wrapf(err, "Error opening log: %s", topic)
			}
		}
	}

	return logs, nil
}

type ByNumericalFilename []os.FileInfo

func (nf ByNumericalFilename) Len() int      { return len(nf) }
func (nf ByNumericalFilename) Swap(i, j int) { nf[i], nf[j] = nf[j], nf[i] }
func (nf ByNumericalFilename) Less(i, j int) bool {

	// Use path names
	pathA := nf[i].Name()
	pathB := nf[j].Name()

	// Grab integer value of each filename by parsing the string and slicing off
	// the extension
	a, err1 := strconv.ParseInt(pathA[0:strings.LastIndex(pathA, ".")], 10, 64)
	b, err2 := strconv.ParseInt(pathB[0:strings.LastIndex(pathB, ".")], 10, 64)

	// If any were not numbers sort lexographically
	if err1 != nil || err2 != nil {
		return pathA < pathB
	}

	// Which integer is smaller?
	return a < b
}

func NewFileLog(logger *zap.SugaredLogger, directory string, topic string, config *pb.TopicConfig) (*FileLog, error) {
	basePath := path.Join(directory, topic)

	ctxLogger := logger.With(
		"topic", topic,
	)

	exists, err := exists(basePath)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := os.Mkdir(basePath, 0755); err != nil {
			return nil, err
		}
	}

	entries, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	sort.Sort(ByNumericalFilename(entries))

	segments := make([]logSegment, 0)

	if len(entries) == 0 {
		// for each segment...
		fls, err := openSegment(logger, config, basePath, 0)
		if err != nil {
			return nil, errors.Wrap(err, "Error creating new segment")
		}

		segments = append(segments, fls)
	} else {
		for _, entry := range entries { // TODO TODO sort numerically!
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
				ctxLogger.Debugw("Opening segment",
					"filename", entry.Name(),
				)
				startOffset, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".log"), 10, 64)
				if err != nil {
					return nil, errors.Wrap(err, "Error parsing log filename")
				}

				fls, err := openSegment(ctxLogger, config, basePath, startOffset)
				if err != nil {
					return nil, errors.Wrapf(err, "Error opening segment: %s/%d.log", basePath, startOffset)
				}

				ctxLogger.Debugw("Opened segment",
					"filename", entry.Name(),
					"startOffset", fls.StartOffset(),
					"endOffset", fls.EndOffset())

				segments = append(segments, fls)
			}
		}
	}

	lastSegment := segments[len(segments)-1]
	nextOffset := lastSegment.EndOffset()

	fl := &FileLog{
		config:              config,
		basePath:            basePath,
		topic:               topic,
		nextOffset:          nextOffset,
		messagesWithOffsets: make(LogEntryChannel),
		logger:              ctxLogger,
		segments:            segments,
		currentSegment:      lastSegment,
		closeCh:             make(chan bool),
	}

	ctxLogger.Infow("Opened log",
		"config", fl.config,
		"nextOffset", nextOffset,
		"segments", len(segments),
		"logsActive", segments[len(segments)-1].EndOffset()-segments[0].StartOffset(),
	)

	go fl.logMaintenance()

	return fl, nil
}

func DefaultConfig() *pb.TopicConfig {
	return &pb.TopicConfig{
		MessageSizeLimit:          4 * 1024 * 1024,
		SegmentSizeLimit:          1 * 1024 * 1024,
		IndexAfterBytes:           4096,
		RotateSegmentAfterSeconds: 24 * 3600,
		StaleSegmentSeconds:       720 * 3600,
		LogMaintenancePeriod:      300,
	}
}
