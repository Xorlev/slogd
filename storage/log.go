package storage

import (
	"bufio"
	"errors"
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

type LogEntryChannel chan *pb.LogEntry

type Log interface {
	LogChannel() LogEntryChannel
	Retrieve(*pb.GetLogsRequest) ([]*pb.LogEntry, error)
	Append([]*pb.LogEntry) ([]uint64, error)
}

type FileLog struct {
	Log

	logger              *zap.SugaredLogger
	basePath            string
	topic               string
	messagesWithOffsets LogEntryChannel
	rwLock              sync.RWMutex
	nextOffset          uint64
	segment             logSegment
}

type logSegment interface {
	append(*pb.LogEntry) error
	flush() error
}

type fileLogSegment struct {
	logSegment

	logger        *zap.SugaredLogger
	basePath      string
	file          *os.File
	filePosition  int64 // TODO: ensure reads never go beyond filePosition
	fileWriter    *bufio.Writer
	segmentWriter WriteCloser
}

func (fl *FileLog) LogChannel() LogEntryChannel {
	return fl.messagesWithOffsets
}

func (fl *FileLog) Retrieve(req *pb.GetLogsRequest) ([]*pb.LogEntry, error) {
	fl.rwLock.RLock()
	defer fl.rwLock.RUnlock()

	// only a copy of the pointers
	// logCopy := make([]*pb.LogEntry, len(fl.logs))
	// copy(logCopy, fl.logs)
	file, err := os.OpenFile(fl.basePath+"/0.log", os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	reader := NewDelimitedReader(bufio.NewReader(file), MESSAGE_SIZE_LIMIT)

	// Stupid inefficient
	// TODO: seek to offset/timestamp bound
	// TODO: only read up to max bytes
	logEntries := make([]*pb.LogEntry, 0)
	scannedLogs := 0
	for {
		msg := &pb.LogEntry{}
		err := reader.ReadMsg(msg)

		if err == nil {
			scannedLogs += 1

			fl.logger.Debugf("Scanning entry: %+v", msg)

			if msg != nil && msg.GetOffset() >= req.GetOffset() {
				logEntries = append(logEntries, msg)
			}

			if int32(len(logEntries)) == req.GetMaxMessages() {
				break
			}
		} else {
			// Found the end of the log
			if err == io.EOF {
				fl.logger.Debugf("End of log, EOF")
				break
			} else {
				return nil, err
			}
		}
	}

	fl.logger.Debugf("Scanned %d logs for %d returned messages",
		scannedLogs, len(logEntries))

	return logEntries, nil
}

func (fl *FileLog) Append(logs []*pb.LogEntry) ([]uint64, error) {
	fl.rwLock.Lock()
	defer fl.rwLock.Unlock()

	newOffsets := make([]uint64, len(logs))
	for i, log := range logs {
		if log != nil {
			fl.logger.Debugf("Appending: %+v", log)
			log.Offset = fl.nextOffset
			newOffsets[i] = fl.nextOffset
			fl.nextOffset += 1

			fl.segment.append(log)
		}
	}

	fl.segment.flush()

	// Publish to stream
	for _, log := range logs {
		fl.messagesWithOffsets <- log
	}

	fl.logger.Debug("Done with Append()")

	return newOffsets, nil
}

func (s *fileLogSegment) append(log *pb.LogEntry) error {
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

func (s *fileLogSegment) flush() error {
	s.fileWriter.Flush()

	// TODO: also sync dir?
	if err := s.file.Sync(); err != nil {
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

	// TODO move to segment
	file, err := os.OpenFile(path.Join(basePath, "/0.log"), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	nextOffset := uint64(0)
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
		segment: &fileLogSegment{
			logger:        logger,
			basePath:      basePath,
			file:          file,
			fileWriter:    fileWriter,
			filePosition:  endOfFile,
			segmentWriter: NewDelimitedWriter(fileWriter),
		},
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
