package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// KV Storage (e.g. key -> filepos, or key -> incremental crc)

const (
	keySize   = 8
	valueSize = 8
	indexSize = keySize + valueSize
)

type kvStore struct {
	sync.RWMutex

	logger       *zap.SugaredLogger
	file         *os.File
	filename     string
	filePosition int64 // TODO: ensure reads never go beyond filePosition
	startKey     uint64
	endKey       uint64

	buffer []byte
}

func (kvs *kvStore) Find(targetKey uint64) (uint64, error) {
	buffer := make([]byte, indexSize)
	file, err := os.OpenFile(kvs.filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return 0, err
	}

	kvs.RLock()
	defer kvs.RUnlock()

	// lowest without going over
	// midpoint
	var lo = 0
	var high = kvs.Size() - 1

	var iterations = 0
	for lo <= high {
		mid := int(math.Ceil(float64(high)/2.0 + float64(lo)/2.0))

		// kvs.logger.Debugf("Searching: lo = %d, high = %d, mid = %d", lo, high, mid)

		foundKey, filePos, err := kvs.readEntryAt(file, int64(mid), buffer)
		if err != nil {
			return 0, err
		}

		// kvs.logger.Debugf("foundKey: %d, targetKey: %d", foundKey, targetKey)

		if foundKey < targetKey {
			// As close as we get!
			if lo == high {
				kvs.logger.Debugf("Found nearest key %d at file position %d", foundKey, filePos)
				return filePos, nil
			}

			// search upper
			lo = mid
		} else if foundKey > targetKey {
			// Not found in index?
			if lo == high {
				kvs.logger.Warnf("Didn't find entry in index: %d", targetKey)
				return 0, nil
			}

			high = mid - 1
		} else if foundKey == targetKey || lo == high {
			// found key exactly
			// kvs.logger.Debugf("Found key %d at file position %d", foundKey, filePos)
			return filePos, nil
		}

		iterations++

		if iterations == 200 {
			return 0, errors.New("Failed to converge on index")
		}
	}

	kvs.logger.Infof("Failed to find key %d in index", targetKey)

	return 0, nil
}

func (kvs *kvStore) readEntryAt(file *os.File, position int64, buffer []byte) (uint64, uint64, error) {
	file.Seek(position*indexSize, io.SeekStart)
	n, err := file.Read(buffer)
	if err != nil {
		return 0, 0, err
	}

	if n < indexSize {
		return 0, 0, errors.New("Incomplete index entry, index corrupt")
	}

	key := binary.BigEndian.Uint64(buffer)
	filePos := binary.BigEndian.Uint64(buffer[8:])

	return key, filePos, nil
}

// key must be a monotonically increasing value
func (kvs *kvStore) IndexKey(key uint64, value int64) error {
	kvs.Lock()
	defer kvs.Unlock()

	// Skip entries which are equal to the latest entry -- for instance, two messages at the same timestamp.
	// This is possible and OK, but we only want to index the first entry since we'll then do a linear scan
	// over the rest of the segment.
	if key == kvs.endKey && kvs.size() > 0 {
		kvs.logger.Debugf("Eliding index write, key is equal to current max key: %d. KV provided: (%d, %d)", key, key, value)
		return nil
	}

	// Keys must not go backwards. This would break the binary search.
	if key < kvs.endKey {
		return errors.Errorf("Key must be a increasing value: %d < %d", key, kvs.endKey)
	}

	if kvs.size() == 0 {
		kvs.startKey = key
	}

	kvs.endKey = key

	kvs.logger.Debugf("Indexing key %d with value %d", key, value)

	binary.BigEndian.PutUint64(kvs.buffer[:keySize], key)
	binary.BigEndian.PutUint64(kvs.buffer[keySize:keySize+valueSize], uint64(value))

	bytesWritten, err := kvs.file.Write(kvs.buffer)
	if err != nil {
		return err
	}

	kvs.logger.Debugf("Wrote %d bytes to index", bytesWritten)

	kvs.filePosition += int64(bytesWritten)

	kvs.flush()

	return nil
}

// Index size in entries
func (kvs *kvStore) Size() int {
	kvs.RLock()
	defer kvs.RUnlock()
	return kvs.size()
}

// Index size in entries
func (kvs *kvStore) size() int {
	return int(kvs.filePosition) / indexSize
}

// Index size in bytes
func (kvs *kvStore) SizeBytes() uint64 {
	kvs.RLock()
	defer kvs.RUnlock()

	return kvs.sizeBytes()
}

// Index size in bytes
func (kvs *kvStore) sizeBytes() uint64 {
	return uint64(kvs.filePosition)
}

func (kvs *kvStore) Truncate() error {
	kvs.Lock()
	defer kvs.Unlock()
	return kvs.truncate()
}

func (kvs *kvStore) truncate() error {
	if err := kvs.file.Truncate(0); err != nil {
		return err
	}
	if _, err := kvs.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func (kvs *kvStore) Delete() error {
	kvs.Lock()
	defer kvs.Unlock()

	return kvs.delete()
}

func (kvs *kvStore) delete() error {
	kvs.Flush()
	if err := kvs.file.Close(); err != nil {
		return err
	}

	if err := os.Remove(kvs.filename); err != nil {
		return errors.Wrap(err, "Unable to remove index.")
	}

	return nil
}

func (kvs *kvStore) Flush() error {
	kvs.Lock()
	defer kvs.Unlock()

	return kvs.flush()
}

func (kvs *kvStore) flush() error {
	if err := kvs.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (kvs *kvStore) Close() error {
	kvs.Lock()
	defer kvs.Unlock()

	kvs.flush()
	if err := kvs.file.Close(); err != nil {
		return err
	}

	return nil
}

// OpenOrCreateStore Opens an existing KVStore or creates a new one for the given path/suffix/startKey
func OpenOrCreateStore(logger *zap.SugaredLogger, basePath string, suffix string, startKey uint64) (*kvStore, error) {
	filename := fmt.Sprintf("/%d.%s", startKey, suffix)
	filename = path.Join(basePath, filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	// TODO: check key not corrupted
	// Iterate over entries checking that both key and file position are monotonically increasing
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

	logger.Debugw("Opening "+suffix,
		"filename", filename,
		"startKey", startKey,
	)

	kvs := &kvStore{
		logger:       logger,
		file:         file,
		filename:     filename,
		filePosition: endOfFile,
		startKey:     0,
		endKey:       0,
		buffer:       make([]byte, indexSize),
	}

	// If index is non-zero sized, figure out the last key we have indexed
	if kvs.Size() > 0 {
		startKey, _, err := kvs.readEntryAt(kvs.file, 0, kvs.buffer)
		if err != nil {
			return nil, err
		}
		endKey, _, err := kvs.readEntryAt(kvs.file, int64(kvs.Size()-1), kvs.buffer)
		if err != nil {
			return nil, err
		}
		kvs.startKey = startKey
		kvs.endKey = endKey
	}

	logger.Debugw("Opened "+suffix,
		"filename", filename,
		"startKey", kvs.startKey,
		"endKey", kvs.endKey,
	)

	return kvs, nil
}
