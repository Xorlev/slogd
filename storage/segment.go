package storage

import (
	"bufio"
	"fmt"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	pb "github.com/xorlev/slogd/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"io"
	"os"
	"path"
	"sync"
	"time"
)

const (
	BYTES_BETWEEN_INDEX = 4096
)

type logSegment interface {
	Retrieve(context.Context, *LogFilter, int64, uint32) ([]*pb.LogEntry, int, int64, error)
	Append(context.Context, *pb.LogEntry) error
	StartOffset() uint64
	EndOffset() uint64
	SizeBytes() uint64
	Flush() error
	Close() error
}

type fileLogSegment struct {
	logSegment
	sync.RWMutex

	logger              *zap.SugaredLogger
	basePath            string
	closed              bool
	file                *os.File
	filename            string
	filePosition        int64 // TODO: ensure reads never go beyond filePosition
	fileWriter          *bufio.Writer
	offsetIndex         Index
	segmentWriter       WriteCloser
	positionOfLastIndex int64

	startOffset uint64
	endOffset   uint64
	minTs       *time.Time
	maxTs       *time.Time
}

func (s *fileLogSegment) Retrieve(ctx context.Context, logFilter *LogFilter, filePosition int64, maxMessages uint32) ([]*pb.LogEntry, int, int64, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, 0, -1, errors.New("Segment is closed.")
	}

	startedAt := time.Now()

	// Open new file handle for seeking
	// TODO(xorlev): can these be safely pooled?
	file, err := os.OpenFile(s.filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, 0, -1, errors.Wrap(err, "Failed to open segment file.")
	}

	// Determine where to start in the file
	var positionStart uint64 = 0
	if filePosition < 0 {
		if logFilter.StartOffset > 0 {
			// Seek to first position
			var err error
			positionStart, err = s.offsetIndex.Find(logFilter.StartOffset)
			if err != nil {
				return nil, 0, -1, errors.Wrap(err, "Failed to search index for start offset.")
			}
		} else if !logFilter.Timestamp.IsZero() {
			s.logger.Warn("Timestamp indices have not yet been implemented, falling back to full log scans.")
		}
	} else {
		// We were provided an explicit position (continuations for cursors), seek there
		positionStart = uint64(filePosition)
	}

	s.logger.Debugf("Seeking to position %v", positionStart)
	file.Seek(int64(positionStart), io.SeekStart)

	// Read and collect messages from log
	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)
	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0
	for {
		// Abort early if we can
		if ctx.Err() != nil {
			return nil, scannedLogs, -1, ctx.Err()
		}

		msg := &pb.LogEntry{}
		err := reader.ReadMsg(msg)

		if err == nil {
			scannedLogs += 1
			// s.logger.Debugf("Scanning entry: %+v", msg)

			// Check that message is inside our offset boundary
			if msg != nil {
				passes, err := logFilter.LogPassesFilter(msg)

				// Failed to run filter, propagate
				if err != nil {
					return nil, 0, -1, errors.Wrap(err, "Failed to run LogFilter over log entry")
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
				s.logger.Debugf("End of segment, EOF")
				break
			} else {
				return nil, scannedLogs, -1, errors.Wrap(err, "Error while scanning segment")
			}
		}
	}

	positionEnd, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, scannedLogs, -1, errors.Wrap(err, "Failed to determine file position after scan")
	}

	s.logger.Infof("Took %+v to scan segment (%d logs scanned, %d bytes).", time.Since(startedAt), scannedLogs, positionEnd-int64(positionStart))

	return logEntries, scannedLogs, positionEnd, nil
}

func (s *fileLogSegment) Append(ctx context.Context, log *pb.LogEntry) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errors.New("Segment is closed")
	}

	// Assign timestamp
	newTime := time.Now()
	if s.maxTs != nil && newTime.Before(*s.maxTs) {
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

	logTimestamp, err := types.TimestampFromProto(log.GetTimestamp())
	if err != nil {
		return err
	}
	if s.minTs == nil || logTimestamp.Before(*s.minTs) {
		s.minTs = &logTimestamp
	}
	if s.maxTs == nil || logTimestamp.After(*s.maxTs) {
		s.maxTs = &logTimestamp
	}

	before := s.filePosition
	s.logger.Debugf("At position: %d pre-append()", s.filePosition)

	bytesWritten, err := s.segmentWriter.WriteMsg(log)

	if err != nil {
		return errors.Wrap(err, "Error writing to segment.")
	}

	s.filePosition += int64(bytesWritten)

	// Index if this is the first item or if it's been BYTES_BETWEEN_INDEX since the last index
	if s.positionOfLastIndex == 0 || s.filePosition-s.positionOfLastIndex >= BYTES_BETWEEN_INDEX {
		s.offsetIndex.IndexOffset(log.GetOffset(), s.filePosition-int64(bytesWritten))
		s.positionOfLastIndex = s.filePosition
	}

	s.logger.Debugf("Appended: %+v, wrote %d bytes.", log, s.filePosition-before)

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

func (s *fileLogSegment) SizeBytes() uint64 {
	s.RLock()
	defer s.RUnlock()

	return uint64(s.filePosition)
}

func (s *fileLogSegment) Flush() error {
	s.fileWriter.Flush()

	// TODO: also sync dir?
	if err := s.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *fileLogSegment) Close() error {
	s.Lock()
	defer s.Unlock()

	s.Flush()
	if err := s.file.Close(); err != nil {
		return err
	}

	if err := s.offsetIndex.Close(); err != nil {
		return err
	}

	s.closed = true

	return nil
}

func openSegment(logger *zap.SugaredLogger, basePath string, startOffset uint64) (logSegment, error) {
	// TODO move to segment
	filename := fmt.Sprintf("/%d.log", startOffset)
	filename = path.Join(basePath, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	ctxLogger := logger.With(
		"filename", filename,
	)

	nextOffset := startOffset
	logEntry := &pb.LogEntry{}
	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)
	var minTs *time.Time = nil
	var maxTs *time.Time = nil
	for {
		err := reader.ReadMsg(logEntry)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		if logEntry.GetOffset() < nextOffset {
			ctxLogger.Errorw("Log entry unexpected",
				"logEntry_offset", logEntry.GetOffset(),
			)
			return nil, errors.New(fmt.Sprintf("Later log entry has lower offset than previous log entry: %d < %d", logEntry.GetOffset(), nextOffset))
		}

		logTimestamp, err := types.TimestampFromProto(logEntry.GetTimestamp())
		if err != nil {
			return nil, err
		}

		if minTs == nil || logTimestamp.Before(*minTs) {
			minTs = &logTimestamp
		}

		if maxTs == nil || logTimestamp.After(*maxTs) {
			maxTs = &logTimestamp
		}

		nextOffset = logEntry.GetOffset() + 1
	}

	// Initialize end of file
	endOfFile, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	fileWriter := bufio.NewWriter(file)

	fls := &fileLogSegment{
		logger:        ctxLogger,
		basePath:      basePath,
		file:          file,
		filename:      filename,
		fileWriter:    fileWriter,
		filePosition:  endOfFile,
		segmentWriter: NewDelimitedWriter(fileWriter),

		startOffset: startOffset,
		endOffset:   nextOffset,
		minTs:       minTs,
		maxTs:       maxTs,
	}

	offsetIndex, err := OpenOffsetIndex(logger, basePath, startOffset)
	if err != nil {
		return nil, err
	}

	fls.offsetIndex = offsetIndex

	if offsetIndex.SizeBytes() == 0 && startOffset != nextOffset {
		// Index is missing
		logger.Infow("Index missing for segment, rebuilding",
			"filename", filename,
		)

		if err := fls.rebuildIndex(); err != nil {
			return nil, err
		}
	}

	return fls, nil
}

func (s *fileLogSegment) rebuildIndex() error {
	// Rewind segment to start of file
	s.file.Seek(0, io.SeekStart)

	var lastIndexPosition int64 = 0
	logEntry := &pb.LogEntry{}
	reader := NewDelimitedReader(bufio.NewReader(s.file), MESSAGE_SIZE_LIMIT)
	for {
		err := reader.ReadMsg(logEntry)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		position, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		if lastIndexPosition == 0 || position-lastIndexPosition >= BYTES_BETWEEN_INDEX {
			if err := s.offsetIndex.IndexOffset(logEntry.GetOffset(), position); err != nil {
				return err
			}
			lastIndexPosition = position
		}
	}

	s.logger.Info("Successfully rebuild index.")

	return nil
}
