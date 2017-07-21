package storage

import (
	"bufio"
	"errors"
	"fmt"
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
	Retrieve(context.Context, uint64, int64, uint32) ([]*pb.LogEntry, int, int64, error)
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
	startOffset         uint64
	endOffset           uint64
	file                *os.File
	filename            string
	filePosition        int64 // TODO: ensure reads never go beyond filePosition
	fileWriter          *bufio.Writer
	offsetIndex         Index
	segmentWriter       WriteCloser
	positionOfLastIndex int64
}

func (s *fileLogSegment) Retrieve(ctx context.Context, startOffset uint64, filePosition int64, maxMessages uint32) ([]*pb.LogEntry, int, int64, error) {
	if s.closed {
		return nil, 0, -1, errors.New("Segment is closed.")
	}

	startedAt := time.Now()
	defer func() {
		s.logger.Infof("Took %+v to scan segment", time.Since(startedAt))
	}()

	// Stupid inefficient
	// TODO: seek to offset/timestamp bound
	file, err := os.OpenFile(s.filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, 0, -1, err
	}

	var positionStart uint64 = 0

	if filePosition < 0 {
		if startOffset > 0 {
			// Seek to first position
			var err error
			positionStart, err = s.offsetIndex.Find(startOffset)
			if err != nil {
				return nil, 0, -1, err
			}
		}
	} else {
		positionStart = uint64(filePosition)
	}

	s.logger.Debugf("Seeking to position %v", positionStart)

	file.Seek(int64(positionStart), io.SeekStart)

	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)

	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0
	for {
		msg := &pb.LogEntry{}
		err := reader.ReadMsg(msg)

		if err == nil {
			scannedLogs += 1

			// s.logger.Debugf("Scanning entry: %+v", msg)

			if msg != nil && msg.GetOffset() >= startOffset {
				logEntries = append(logEntries, msg)
			}

			if uint32(len(logEntries)) == maxMessages {
				break
			}
		} else {
			// Found the end of the log
			if err == io.EOF {
				s.logger.Debugf("End of log, EOF")
				break
			} else {
				return nil, scannedLogs, -1, err
			}
		}
	}

	finalPosition, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, -1, err
	}

	return logEntries, scannedLogs, finalPosition, nil
}

func (s *fileLogSegment) Append(ctx context.Context, log *pb.LogEntry) error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errors.New("Segment is closed.")
	}

	if log.GetOffset() < s.endOffset {
		return errors.New("Tried to append log with offset less than max offset to log segment")
	} else {
		s.endOffset = log.GetOffset()
	}

	before := s.filePosition
	s.logger.Debugf("At position: %d pre-append()", s.filePosition)

	bytesWritten, err := s.segmentWriter.WriteMsg(log)

	if err != nil {
		return err
	}

	s.filePosition += int64(bytesWritten)

	// Index if necessary
	if s.filePosition-s.positionOfLastIndex >= BYTES_BETWEEN_INDEX {
		s.offsetIndex.IndexOffset(log.GetOffset(), s.filePosition-int64(bytesWritten))
		s.positionOfLastIndex = s.filePosition
	}

	s.logger.Debugf("Appended: %+v, wrote %d bytes", log, s.filePosition-before)

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

	nextOffset := startOffset
	logEntry := &pb.LogEntry{}
	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)
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
			logger.Errorw("Log entry unexpected",
				"basePath", basePath,
				"startOffset", startOffset,
				"logEntry_offset", logEntry.GetOffset(),
			)
			return nil, errors.New(fmt.Sprintf("Later log entry has lower offset than previous log entry: %d < %d", logEntry.GetOffset(), nextOffset))
		}

		nextOffset = logEntry.GetOffset() + 1
	}

	// Initialize end of file
	endOfFile, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	fileWriter := bufio.NewWriter(file)

	offsetIndex, err := OpenOffsetIndex(logger, basePath, startOffset)
	if err != nil {
		return nil, err
	}

	return &fileLogSegment{
		logger:        logger,
		basePath:      basePath,
		startOffset:   startOffset,
		endOffset:     nextOffset,
		file:          file,
		filename:      filename,
		fileWriter:    fileWriter,
		filePosition:  endOfFile,
		offsetIndex:   offsetIndex,
		segmentWriter: NewDelimitedWriter(fileWriter),
	}, nil
}
