package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"math"
	"os"
	"path"
	"sync"
)

type Index interface {
	Find(offset uint64) (uint64, error)
	IndexOffset(offset uint64, position int64) error
	Size() int
	SizeBytes() uint64
	Truncate() error
	Delete() error
	Flush() error
	Close() error
}

const (
	OFFSET_SIZE        = 8
	FILE_POSITION_SIZE = 8
	INDEX_SIZE         = OFFSET_SIZE + FILE_POSITION_SIZE
)

type fileIndex struct {
	Index
	sync.RWMutex

	logger       *zap.SugaredLogger
	file         *os.File
	filename     string
	filePosition int64 // TODO: ensure reads never go beyond filePosition
	startOffset  uint64
	endOffset    uint64

	buffer []byte
}

func (fi *fileIndex) Find(targetOffset uint64) (uint64, error) {
	buffer := make([]byte, INDEX_SIZE)
	file, err := os.OpenFile(fi.filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return 0, err
	}

	fi.RLock()
	defer fi.RUnlock()

	// lowest without going over
	// midpoint
	var lo = 0
	var high = fi.Size() - 1

	var iterations = 0
	for lo <= high {
		mid := int(math.Ceil(float64(high)/2.0 + float64(lo)/2.0))

		fi.logger.Debugf("Searching: lo = %d, high = %d, mid = %d", lo, high, mid)

		foundOffset, filePos, err := fi.readEntryAt(file, int64(mid), buffer)
		if err != nil {
			return 0, err
		}

		fi.logger.Debugf("foundOffset: %d, targetOffset: %d", foundOffset, targetOffset)

		if foundOffset < targetOffset {
			// As close as we get!
			if lo == high {
				fi.logger.Debugf("Found nearest offset %d at file position %d", foundOffset, filePos)
				return filePos, nil
			}

			// search upper
			lo = mid
		} else if foundOffset > targetOffset {
			// Not found in index?
			if lo == high {
				fi.logger.Warnf("Didn't find entry in index: %d", targetOffset)
				return 0, nil
			}

			high = mid - 1
		} else if foundOffset == targetOffset || lo == high {
			// found offset exactly
			fi.logger.Debugf("Found offset %d at file position %d", foundOffset, filePos)
			return filePos, nil
		}

		iterations += 1

		if iterations == 200 {
			return 0, errors.New("Failed to converge on index")
		}
	}

	fi.logger.Debugf("Failed to find offset %d in index", targetOffset)

	return 0, nil
}

func (fi *fileIndex) readEntryAt(file *os.File, position int64, buffer []byte) (uint64, uint64, error) {
	file.Seek(position*INDEX_SIZE, io.SeekStart)
	n, err := file.Read(buffer)
	if err != nil {
		return 0, 0, err
	}

	if n < INDEX_SIZE {
		return 0, 0, errors.New("Incomplete index entry.")
	}

	offset := binary.BigEndian.Uint64(buffer)
	filePos := binary.BigEndian.Uint64(buffer[8:])

	return offset, filePos, nil
}

func (fi *fileIndex) IndexOffset(offset uint64, position int64) error {
	fi.Lock()
	defer fi.Unlock()

	fi.logger.Debugf("Indexing offset %d at file position %d", offset, position)

	binary.BigEndian.PutUint64(fi.buffer[:OFFSET_SIZE], offset)
	binary.BigEndian.PutUint64(fi.buffer[OFFSET_SIZE:OFFSET_SIZE+FILE_POSITION_SIZE], uint64(position))

	bytesWritten, err := fi.file.Write(fi.buffer)
	if err != nil {
		return err
	}

	fi.logger.Debugf("Wrote %d bytes to index", bytesWritten)

	fi.filePosition += int64(bytesWritten)

	fi.Flush()

	return nil
}

// Index size in entries
func (fi *fileIndex) Size() int {
	fi.RLock()
	defer fi.RUnlock()
	return int(fi.filePosition) / INDEX_SIZE
}

// Index size in bytes
func (fi *fileIndex) SizeBytes() uint64 {
	fi.RLock()
	defer fi.RUnlock()

	return uint64(fi.filePosition)
}

func (fi *fileIndex) Truncate() error {
	fi.Lock()
	defer fi.Unlock()
	if err := fi.file.Truncate(0); err != nil {
		return err
	}
	if _, err := fi.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func (fi *fileIndex) Delete() error {
	fi.Lock()
	defer fi.Unlock()

	fi.Flush()
	if err := fi.file.Close(); err != nil {
		return err
	}

	if err := os.Remove(fi.filename); err != nil {
		return errors.Wrap(err, "Unable to remove index.")
	}

	return nil
}

func (fi *fileIndex) Flush() error {
	// TODO: also sync dir?
	if err := fi.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (fi *fileIndex) Close() error {
	fi.Lock()
	defer fi.Unlock()

	fi.Flush()
	if err := fi.file.Close(); err != nil {
		return err
	}

	return nil
}

func OpenOffsetIndex(logger *zap.SugaredLogger, basePath string, startOffset uint64) (*fileIndex, error) {
	filename := fmt.Sprintf("/%d.oindex", startOffset)
	filename = path.Join(basePath, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	// TODO: check offset not corrupted
	// Iterate over entries checking that both offset and file position are monotonically increasing
	// Also ensure index size is expected; correct number of entries
	// Otherwise truncate & rebuild

	// Initialize end of file
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	endOfFile, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	logger.Debugw("Opening index",
		"filename", filename,
		"startOffset", startOffset,
	)

	fi := &fileIndex{
		logger:       logger,
		file:         file,
		filename:     filename,
		filePosition: endOfFile,
		startOffset:  startOffset,
		endOffset:    startOffset,
		buffer:       make([]byte, INDEX_SIZE),
	}

	// If index is non-zero sized, figure out the last offset we have indexed
	if fi.Size() > 0 {
		offset, _, err := fi.readEntryAt(fi.file, int64(fi.Size()-1), fi.buffer)
		if err != nil {
			return nil, err
		}
		fi.endOffset = offset
	}

	logger.Debugw("Opened index",
		"filename", filename,
		"startOffset", startOffset,
		"endOffset", fi.endOffset,
	)

	return fi, nil
}
