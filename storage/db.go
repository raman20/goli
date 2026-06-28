package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"
)

var (
	ErrDBClosed = errors.New("database is closed")
	ErrKeyEmpty = errors.New("key cannot be empty")
)

type DB struct {
	Name       string
	segmentMgr *SegmentManager
	index      Index
	closed     bool
	closeOnce  sync.Once
	options    Options
	walDir     string
	sstDir     string
	segmentDir string
	mu         sync.RWMutex
}

type Options struct {
	MemtableSize        int64
	MaxConcurrency      int
	SyncWrites          bool
	DataDir             string
	CompactionThreshold int
}

func DefaultOptions() Options {
	return Options{
		MemtableSize:        32 * 1024 * 1024, // 32MB
		MaxConcurrency:      runtime.GOMAXPROCS(0),
		SyncWrites:          true,
		DataDir:             "data",
		CompactionThreshold: 4,
	}
}

func Open(name string, opts Options, index Index) (*DB, error) {
	if name == "" {
		return nil, ErrKeyEmpty
	}

	// Create directory structure
	dbPath := filepath.Join(opts.DataDir, name)
	walPath := filepath.Join(dbPath, "wal")
	sstPath := filepath.Join(dbPath, "sst")
	segmentPath := filepath.Join(dbPath, "segments")

	for _, dir := range []string{dbPath, walPath, sstPath, segmentPath} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// 1. Initialize Segment Manager
	sm, err := NewSegmentManager(segmentPath, opts.MemtableSize*2)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SegmentManager: %w", err)
	}

	db := &DB{
		Name:       name,
		options:    opts,
		walDir:     walPath,
		sstDir:     sstPath,
		segmentDir: segmentPath,
		segmentMgr: sm,
		index:      index,
	}

	return db, nil
}

func encodeRecord(key, value string) []byte {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))
	buf := make([]byte, 8+keyLen+valLen)
	binary.BigEndian.PutUint32(buf[0:4], keyLen)
	binary.BigEndian.PutUint32(buf[4:8], valLen)
	copy(buf[8:8+keyLen], key)
	copy(buf[8+keyLen:], value)
	return buf
}

func decodeValue(record []byte) string {
	if len(record) < 8 {
		return ""
	}
	keyLen := binary.BigEndian.Uint32(record[0:4])
	valLen := binary.BigEndian.Uint32(record[4:8])
	if int(8+keyLen+valLen) > len(record) {
		return ""
	}
	if valLen == 0 {
		return ""
	}
	return unsafe.String(&record[8+keyLen], valLen)
}

func (db *DB) Set(key string, value string) error {
	if key == "" {
		return ErrKeyEmpty
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	// 1. Write payload sequentially to Segment Manager
	payload := encodeRecord(key, value)
	ref, err := db.segmentMgr.Append(payload)
	if err != nil {
		return fmt.Errorf("failed to append to SegmentManager: %w", err)
	}

	// 2. Map Key -> RecordRef in the index
	return db.index.Put([]byte(key), ref)
}

func (db *DB) Get(key string) (string, bool) {
	if key == "" {
		return "", false
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return "", false
	}

	// 1. Query index for coordinate
	ref, found, err := db.index.Get([]byte(key))
	if err != nil || !found {
		return "", false
	}

	// 2. Fetch value from Segment Manager using coordinate
	record, err := db.segmentMgr.Read(ref)
	if err != nil {
		return "", false
	}

	return decodeValue(record), true
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	return db.index.Delete([]byte(key))
}

func (db *DB) Scan(prefix string) (map[string]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	// 1. Query index for matching RecordRefs
	refs, err := db.index.Scan([]byte(prefix))
	if err != nil {
		return nil, err
	}

	// 2. Resolve coordinates to values
	results := make(map[string]string)
	for _, ref := range refs {
		record, err := db.segmentMgr.Read(ref)
		if err != nil {
			continue
		}

		keyLen := binary.BigEndian.Uint32(record[0:4])
		valLen := binary.BigEndian.Uint32(record[4:8])
		
		var k, v string
		if keyLen > 0 {
			k = unsafe.String(&record[8], keyLen)
		}
		if valLen > 0 {
			v = unsafe.String(&record[8+keyLen], valLen)
		}
		results[k] = v
	}

	return results, nil
}

func (db *DB) Close() error {
	var firstErr error
	db.closeOnce.Do(func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		db.closed = true

		if err := db.index.Close(); err != nil {
			firstErr = fmt.Errorf("failed to close Index: %w", err)
		}

		if err := db.segmentMgr.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close SegmentManager: %w", err)
		}
	})
	return firstErr
}

type DBStats struct {
	MemtableSize   int64
	ImmutableCount int
	SSTableCount   int
	SSTableFiles   []string
}

func (db *DB) Stats() DBStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	istats := db.index.Stats()

	return DBStats{
		MemtableSize:   istats.MemtableSize,
		ImmutableCount: istats.ImmutableCount,
		SSTableCount:   istats.SSTableCount,
		SSTableFiles:   istats.SSTableFiles,
	}
}
