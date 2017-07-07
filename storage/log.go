package storage

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/xorlev/slogd/internal"
	pb "github.com/xorlev/slogd/proto"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

const (
	MESSAGE_SIZE_LIMIT = 16 * 1024 * 1024
)

type LogEntryChannel chan *internal.TopicAndLog

type Log interface {
	LogChannel() LogEntryChannel
	Retrieve(*pb.GetLogsRequest) ([]*pb.LogEntry, error)
	Append([]*pb.LogEntry) ([]uint64, error)
}

type FileLog struct {
	Log
	sync.RWMutex

	logger              *zap.SugaredLogger
	basePath            string
	topic               string
	messagesWithOffsets LogEntryChannel
	nextOffset          uint64
	currentSegment      logSegment
}

type logSegment interface {
	Retrieve(startOffset uint64, messagesToRead int32) ([]*pb.LogEntry, int, error)
	Append(*pb.LogEntry) error
	StartOffset() uint64
	EndOffset() uint64
	Flush() error
	Close() error
}

type fileLogSegment struct {
	logSegment

	logger        *zap.SugaredLogger
	basePath      string
	startOffset   uint64
	endOffset     uint64
	file          *os.File
	filePosition  int64 // TODO: ensure reads never go beyond filePosition
	fileWriter    *bufio.Writer
	segmentWriter WriteCloser
}

func (fl *FileLog) LogChannel() LogEntryChannel {
	return fl.messagesWithOffsets
}

func (fl *FileLog) Retrieve(req *pb.GetLogsRequest) ([]*pb.LogEntry, error) {
	// Find covering set of log segments
	segments := []logSegment{fl.currentSegment}

	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0
	for _, segment := range segments {
		segmentLogs, scanned, err := segment.Retrieve(req.GetOffset(), req.GetMaxMessages())
		if err != nil {
			return nil, err
		}

		logEntries = append(logEntries, segmentLogs...)
		scannedLogs += scanned
	}

	fl.logger.Debugf("Scanned %d logs for %d returned messages",
		scannedLogs, len(logEntries))

	return logEntries, nil
}

func (fl *FileLog) Append(logs []*pb.LogEntry) ([]uint64, error) {
	fl.Lock()
	defer fl.Unlock()

	newOffsets := make([]uint64, len(logs))
	for i, log := range logs {
		if log != nil {
			fl.logger.Debugf("Appending: %+v", log)
			log.Offset = fl.nextOffset
			newOffsets[i] = fl.nextOffset
			fl.nextOffset += 1

			fl.currentSegment.Append(log)
		}
	}

	fl.currentSegment.Flush()

	// Publish to stream
	for _, log := range logs {
		fl.messagesWithOffsets <- &internal.TopicAndLog{
			Topic:    fl.topic,
			LogEntry: log,
		}
	}

	fl.logger.Debug("Done with Append()")

	return newOffsets, nil
}

func (s *fileLogSegment) Retrieve(startOffset uint64, maxMessages int32) ([]*pb.LogEntry, int, error) {
	// Stupid inefficient
	// TODO: seek to offset/timestamp bound
	// TODO: only read up to max bytes
	file, err := os.OpenFile(s.basePath+"/0.log", os.O_RDONLY, 0666)
	if err != nil {
		return nil, 0, err
	}

	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)

	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0
	for {
		msg := &pb.LogEntry{}
		err := reader.ReadMsg(msg)

		if err == nil {
			scannedLogs += 1

			s.logger.Debugf("Scanning entry: %+v", msg)

			if msg != nil && msg.GetOffset() >= startOffset {
				logEntries = append(logEntries, msg)
			}

			if int32(len(logEntries)) == maxMessages {
				break
			}
		} else {
			// Found the end of the log
			if err == io.EOF {
				s.logger.Debugf("End of log, EOF")
				break
			} else {
				return nil, scannedLogs, err
			}
		}
	}

	return logEntries, scannedLogs, nil
}

func (s *fileLogSegment) Append(log *pb.LogEntry) error {
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

	s.logger.Debugf("Appended: %+v, wrote %d bytes", log, s.filePosition-before)

	return nil
}

func (s *fileLogSegment) StartOffset() uint64 {
	return s.startOffset
}

func (s *fileLogSegment) EndOffset() uint64 {
	return s.endOffset
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
	s.Flush()
	if err := s.file.Close(); err != nil {
		return err
	}

	return nil
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

			logs[topic], err = NewFileLog(logger, directory, topic)
			if err != nil {
				return nil, err
			}
		}
	}

	return logs, nil
}

func NewFileLog(logger *zap.SugaredLogger, directory string, topic string) (*FileLog, error) {
	basePath := path.Join(directory, topic)

	exists, err := exists(basePath)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := os.Mkdir(basePath, 0755); err != nil {
			return nil, err
		}
	}

	// for each segment...
	fls, err := openSegment(logger, basePath, 0)
	if err != nil {
		return nil, err
	}

	nextOffset := fls.EndOffset()

	logger.Infow("Initializing log",
		"basePath", basePath,
		"topic", topic,
		"nextOffset", nextOffset,
	)
	return &FileLog{
		basePath:            basePath,
		topic:               topic,
		nextOffset:          nextOffset,
		messagesWithOffsets: make(LogEntryChannel),
		logger:              logger,
		currentSegment:      fls,
	}, nil
}

func openSegment(logger *zap.SugaredLogger, basePath string, startOffset uint64) (logSegment, error) {
	// TODO move to segment
	filename := fmt.Sprintf("/%d.log", startOffset)
	file, err := os.OpenFile(path.Join(basePath, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
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
			return nil, errors.New("Later log entry has lower offset than previous log entry")
		}

		nextOffset = logEntry.GetOffset() + 1
	}

	// Initialize end of file
	endOfFile, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	fileWriter := bufio.NewWriter(file)

	return &fileLogSegment{
		logger:        logger,
		basePath:      basePath,
		startOffset:   startOffset,
		endOffset:     nextOffset,
		file:          file,
		fileWriter:    fileWriter,
		filePosition:  endOfFile,
		segmentWriter: NewDelimitedWriter(fileWriter),
	}, nil
}

// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
