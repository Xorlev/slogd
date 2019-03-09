package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	pb "github.com/xorlev/slogd/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type logSegment interface {
	Retrieve(context.Context, *LogQuery, int64, uint32) ([]*pb.LogEntry, int, int64, error)
	Append(context.Context, *pb.LogEntry) error
	StartOffset() uint64
	EndOffset() uint64
	StartTime() time.Time
	EndTime() time.Time
	SizeBytes() uint64
	Delete() error
	Flush() error
	Close() error
}

type fileLogSegment struct {
	logSegment
	sync.RWMutex

	config *pb.TopicConfig
	logger *zap.SugaredLogger

	basePath          string
	closed            bool
	file              *os.File
	filename          string
	endOfFilePosition int64 // TODO: ensure reads never go beyond endOfFilePosition
	fileWriter        *bufio.Writer
	protoFileWriter   WriteCloser

	offsetIndex    *kvStore
	timestampIndex *kvStore
	lastIndexPosition int64


	startOffset uint64
	endOffset   uint64
	startTime   *time.Time
	endTime     *time.Time
}

func (s *fileLogSegment) Retrieve(ctx context.Context, logQuery *LogQuery, continuationFilePosition int64, maxMessages uint32) ([]*pb.LogEntry, int, int64, error) {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return retrieveError(errors.New("segment is closed"))
	}

	s.logger.Debugf("query = %+v", logQuery)

	// Elide read, at end of segment
	if continuationFilePosition == s.endOfFilePosition {
		s.RUnlock()
		return []*pb.LogEntry{}, 0, s.endOfFilePosition, nil
	}
	s.RUnlock()

	startedAt := time.Now()

	// Determine where to start in the file
	var positionStart int64 = 0

	if continuationFilePosition < 0 {
		// Rewrite timestamp queries in terms of StartOffset.
		if !logQuery.Timestamp.IsZero() {
			offset, err := s.timestampIndex.Find(uint64(logQuery.Timestamp.UnixNano()))
			if err != nil {
				return retrieveError(errors.Wrap(err, "Failed to search index for start timestamp."))
			}

			logQuery.StartOffset = offset
		}

		// Skip the index lookup if the query's start offset is the start of the segment.
		if logQuery.StartOffset != s.startOffset {
			p, err := s.offsetIndex.Find(logQuery.StartOffset)
			if err != nil {
				return retrieveError(errors.Wrap(err, "Failed to search index for start offset."))
			}
			positionStart = int64(p)
			s.logger.Infof("Took %+v to lookup index (%d logs scanned, %d bytes read, %d bytes used).",
				time.Since(startedAt))
		}
	} else if logQuery.StartOffset != s.startOffset {
		s.logger.Debugf("Skipping index lookup, %v", logQuery)
		// We were provided an explicit position (continuations for cursors), seek there
		positionStart = continuationFilePosition
	}

	// Read and collect messages from log
	r := io.NewSectionReader(s.file, positionStart, s.endOfFilePosition - positionStart)
	reader := NewDelimitedReader(bufio.NewReader(r), int(s.config.MessageSizeLimit))
	logEntries := make([]*pb.LogEntry, 0)
	var bytesRead int64 = 0
	scannedLogs := 0
	for {
		// Abort early if we can
		if ctx.Err() != nil {
			return retrieveError(ctx.Err())
		}

		msg := &pb.LogEntry{}
		n, err := reader.ReadMsg(msg)

		if err == nil {
			bytesRead += int64(n)
			scannedLogs += 1

			// Check that message is inside our offset boundary
			if msg != nil {
				passes, err := logQuery.LogPassesFilter(msg)

				// Failed to run filter, propagate
				if err != nil {
					return retrieveError(errors.Wrap(err, "Failed to run LogFilter over log entry"))
				}

				if passes {
					logEntries = append(logEntries, msg)
				}
			}

			if uint32(len(logEntries)) == maxMessages {
				break
			}
		} else {
			// Found the end of the log
			if err == io.EOF {
				s.logger.Debugf("End of segment, EOF %d", positionStart+bytesRead)
				break
			} else {
				return retrieveError(errors.Wrap(err, "Error while scanning segment"))
			}
		}
	}

	positionEnd := positionStart + bytesRead
	s.logger.Infof("Took %+v to scan segment (%d logs scanned, %d bytes read, %d bytes used).",
		time.Since(startedAt), scannedLogs, bytesRead, bytesRead)

	return logEntries, scannedLogs, positionEnd, nil
}

func retrieveError(err error) ([]*pb.LogEntry, int, int64, error) {
	return nil, 0, -1, err
}

func (s *fileLogSegment) Append(ctx context.Context, log *pb.LogEntry) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errors.New("Segment is closed")
	}

	// Assign timestamp
	newTime := ctx.Value("requestStart").(time.Time)
	if s.endTime != nil && newTime.Before(*s.endTime) {
		return errors.New("Time moving backwards!")
	}

	ts, err := types.TimestampProto(newTime)
	if err != nil {
		return err
	}

	log.Timestamp = ts

	if log.GetOffset() < s.endOffset {
		return errors.New("Tried to append log with offset less than max offset to log segment")
	} else {
		s.endOffset = log.GetOffset()
	}

	if s.startTime.IsZero() || newTime.Before(*s.startTime) {
		s.startTime = &newTime
	}
	if s.endTime.IsZero() || newTime.After(*s.endTime) {
		s.endTime = &newTime
	}

	before := s.endOfFilePosition
	s.logger.Debugf("At position: %d pre-append()", s.endOfFilePosition)

	bytesWritten, err := s.protoFileWriter.WriteMsg(log)
	if err != nil {
		return errors.Wrap(err, "Error writing to segment.")
	}
	s.endOfFilePosition += int64(bytesWritten)

	// Index if this is the first item or if it's been BYTES_BETWEEN_INDEX since the last index
	if s.lastIndexPosition == 0 || s.endOfFilePosition-s.lastIndexPosition >= int64(s.config.IndexAfterBytes) {
		filepos := s.endOfFilePosition - int64(bytesWritten)

		if err := s.offsetIndex.IndexKey(log.GetOffset(), filepos); err != nil {
			return err
		}
		if err := s.timestampIndex.IndexKey(uint64(newTime.UnixNano()), int64(log.GetOffset())); err != nil {
			return err
		}

		s.lastIndexPosition = s.endOfFilePosition
	}

	s.logger.Debugf("Appended: %+v, wrote %d bytes.", log, s.endOfFilePosition-before)

	return nil
}

func (s *fileLogSegment) StartOffset() uint64 {
	s.RLock()
	defer s.RUnlock()

	return s.startOffset
}

func (s *fileLogSegment) EndOffset() uint64 {
	s.RLock()
	defer s.RUnlock()

	return s.endOffset
}

func (s *fileLogSegment) StartTime() time.Time {
	s.RLock()
	defer s.RUnlock()

	return *s.startTime
}

func (s *fileLogSegment) EndTime() time.Time {
	s.RLock()
	defer s.RUnlock()

	return *s.endTime
}

func (s *fileLogSegment) SizeBytes() uint64 {
	s.RLock()
	defer s.RUnlock()

	return uint64(s.endOfFilePosition)
}

func (s *fileLogSegment) Delete() error {
	s.Lock()
	defer s.Unlock()

	s.flush()
	if err := s.file.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.filename); err != nil {
		return errors.Wrap(err, "Unable to remove segment.")
	}

	if err := s.offsetIndex.Delete(); err != nil {
		return err
	}

	if err := s.timestampIndex.Delete(); err != nil {
		return err
	}

	return nil
}

func (s *fileLogSegment) Flush() error {
	s.Lock()
	defer s.Unlock()

	return s.flush()
}

func (s *fileLogSegment) flush() error {
	if err := s.fileWriter.Flush(); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *fileLogSegment) Close() error {
	s.Lock()
	defer s.Unlock()

	s.flush()
	if err := s.file.Close(); err != nil {
		return err
	}

	if err := s.offsetIndex.Close(); err != nil {
		return err
	}

	if err := s.timestampIndex.Close(); err != nil {
		return err
	}

	s.closed = true

	return nil
}

func openSegment(logger *zap.SugaredLogger, config *pb.TopicConfig, basePath string, startOffset uint64) (logSegment, error) {
	// TODO move to segment
	filename := fmt.Sprintf("/%d.log", startOffset)
	filename = path.Join(basePath, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	parentDir, err := os.OpenFile(basePath, 0, 0666)
	if err != nil {
		return nil, err
	}

	// If we've just created the file, ensure the file metadata is persisted to disk. This requires fsyncing the directory.
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := parentDir.Sync(); err != nil {
		return nil, err
	}

	ctxLogger := logger.With(
		"filename", filename,
	)

	maxOffset := startOffset
	logEntry := &pb.LogEntry{}
	reader := NewDelimitedReader(bufio.NewReader(file), int(config.MessageSizeLimit))
	var startTime *time.Time = nil
	var endTime *time.Time = nil
	for {
		_, err := reader.ReadMsg(logEntry)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		if logEntry.GetOffset() < maxOffset {
			ctxLogger.Errorw("Log entry unexpected",
				"logEntry_offset", logEntry.GetOffset(),
			)
			return nil, errors.New(fmt.Sprintf("Later log entry has lower offset than previous log entry: %d < %d",
				logEntry.GetOffset(), maxOffset))
		}

		logTimestamp, err := types.TimestampFromProto(logEntry.GetTimestamp())
		if err != nil {
			return nil, err
		}

		if startTime == nil || logTimestamp.Before(*startTime) {
			startTime = &logTimestamp
		}

		if endTime == nil || logTimestamp.After(*endTime) {
			endTime = &logTimestamp
		}

		maxOffset = logEntry.GetOffset()
	}

	if startTime == nil {
		startTime = &time.Time{}
		endTime = &time.Time{}
	}

	// Initialize end of file
	endOfFile, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	fileWriter := bufio.NewWriter(file)

	fls := &fileLogSegment{
		config:            config,
		logger:            ctxLogger,
		basePath:          basePath,
		file:              file,
		filename:          filename,
		fileWriter:        fileWriter,
		endOfFilePosition: endOfFile,
		protoFileWriter:   NewDelimitedWriter(fileWriter),

		startOffset: startOffset,
		endOffset:   maxOffset,
		startTime:   startTime,
		endTime:     endTime,
	}

	offsetIndex, err := fls.openOrRebuildIndex("oindex", func(log *pb.LogEntry) uint64 {
		return log.GetOffset()
	}, func(pos int64, log *pb.LogEntry) int64 { return pos })

	if err != nil {
		return nil, err
	}

	tsIndex, err := fls.openOrRebuildIndex("tindex", func(log *pb.LogEntry) uint64 {
		logTimestamp, _ := types.TimestampFromProto(log.GetTimestamp())

		return uint64(logTimestamp.UnixNano())
	}, func(pos int64, log *pb.LogEntry) int64 { return int64(log.GetOffset()) })

	if err != nil {
		return nil, err
	}

	fls.offsetIndex = offsetIndex
	fls.timestampIndex = tsIndex

	return fls, nil
}

func (s *fileLogSegment) openOrRebuildIndex(indexType string, keyFn func(*pb.LogEntry) uint64, valueFn func(int64, *pb.LogEntry) int64) (*kvStore, error) {
	index, err := OpenOrCreateStore(s.logger, s.basePath, indexType, s.startOffset)
	if err != nil {
		return nil, err
	}

	if index.SizeBytes() == 0 && s.startOffset != s.endOffset {
		// Index is missing
		s.logger.Infow("Index missing for segment, rebuilding",
			"filename", s.filename,
		)

		if err := s.rebuildIndex(index, keyFn, valueFn); err != nil {
			return nil, err
		}
	}

	return index, nil
}

func (s *fileLogSegment) rebuildIndex(store *kvStore, keyFn func(*pb.LogEntry) uint64, valueFn func(int64, *pb.LogEntry) int64) error {
	// Rewind segment to start of file
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var lastIndexPosition int64

	reader := NewDelimitedReader(bufio.NewReader(s.file), int(s.config.MessageSizeLimit))
	logEntry := &pb.LogEntry{}
	position := int64(0)
	for {
		n, err := reader.ReadMsg(logEntry)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		if lastIndexPosition == 0 || position-lastIndexPosition >= int64(s.config.IndexAfterBytes) {
			if err := store.IndexKey(keyFn(logEntry), valueFn(position, logEntry)); err != nil {
				return err
			}
			lastIndexPosition = position
		}

		position += int64(n)
	}

	s.logger.Info("Successfully rebuilt index.")

	return nil
}
