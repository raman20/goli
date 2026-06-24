package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrDBClosed = errors.New("database is closed")
	ErrKeyEmpty = errors.New("key cannot be empty")
)

type DB struct {
	Name         string
	currMemtable *Memtable   // Active memtable for writes
	immutable    []*Memtable // Slice of immutable memtables pending flush
	sstables     []*SSTable  // Active SSTables ordered newest-first
	mu           sync.RWMutex
	closed       bool
	closeOnce    sync.Once
	options      Options
	walDir       string
	sstDir       string
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

func Open(name string, opts Options) (*DB, error) {
	if name == "" {
		return nil, ErrKeyEmpty
	}

	if opts.CompactionThreshold <= 1 {
		opts.CompactionThreshold = 4
	}

	// Create directory structure
	dbPath := filepath.Join(opts.DataDir, name)
	walPath := filepath.Join(dbPath, "wal")
	sstPath := filepath.Join(dbPath, "sst")

	for _, dir := range []string{dbPath, walPath, sstPath} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	db := &DB{
		Name:    name,
		options: opts,
		walDir:  walPath,
		sstDir:  sstPath,
	}

	// Recover existing WAL files
	wals, err := os.ReadDir(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Recover memtables from existing WAL files
	for _, wal := range wals {
		if !wal.IsDir() {
			mt, err := InitMemtable(filepath.Join(walPath, wal.Name()), opts.MemtableSize)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize memtable from WAL %s: %w", wal.Name(), err)
			}
			db.immutable = append(db.immutable, mt) // Add to immutable slice
		}
	}

	// Load existing SSTables from sstDir
	ssts, err := os.ReadDir(sstPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SST directory: %w", err)
	}

	var sstFiles []string
	for _, entry := range ssts {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sst" {
			sstFiles = append(sstFiles, entry.Name())
		}
	}

	// Sort SSTable files alphabetically (which matches chronological order due to UnixNano timestamp prefix)
	sort.Strings(sstFiles)

	// Open and load SSTables in reverse order (newest first in db.sstables)
	for i := len(sstFiles) - 1; i >= 0; i-- {
		sstPathFile := filepath.Join(sstPath, sstFiles[i])
		sst, err := OpenSSTable(sstPathFile)
		if err != nil {
			// Close already opened ones
			for _, opened := range db.sstables {
				opened.Close()
			}
			return nil, fmt.Errorf("failed to open SSTable %s: %w", sstFiles[i], err)
		}
		db.sstables = append(db.sstables, sst)
	}

	// Create new active memtable
	if err := db.rotateMemtable(); err != nil {
		// Close already opened SSTables
		for _, opened := range db.sstables {
			opened.Close()
		}
		return nil, fmt.Errorf("failed to create initial memtable: %w", err)
	}

	return db, nil
}

// rotateMemtable creates a new memtable and makes it the current one
func (db *DB) rotateMemtable() error {
	walFile := filepath.Join(db.walDir, uuid.NewString()+".log")
	newMemtable, err := InitMemtable(walFile, db.options.MemtableSize)
	if err != nil {
		return fmt.Errorf("failed to create new memtable: %w", err)
	}

	// If there's an existing current memtable, make it immutable
	if db.currMemtable != nil {
		db.immutable = append(db.immutable, db.currMemtable) // Add to immutable slice
		// Trigger background flush
		go db.flushImmutableMemtables()
	}

	db.currMemtable = newMemtable
	return nil
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

	err := db.currMemtable.Set(key, value)
	if err != nil {
		// If memtable is full, rotate and retry
		if errors.Is(err, ErrMemtableFull) {
			if err := db.rotateMemtable(); err != nil {
				return fmt.Errorf("failed to rotate memtable: %w", err)
			}
			// Retry with new memtable
			return db.currMemtable.Set(key, value)
		}
		return err
	}

	return nil
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

	// First check current memtable
	if value, found := db.currMemtable.Get(key); found {
		if value == "" {
			return "", false // Tombstone (deleted)
		}
		return value, true
	}

	// Then check all immutable memtables (newest to oldest)
	for i := len(db.immutable) - 1; i >= 0; i-- {
		mt := db.immutable[i]
		if value, found := mt.Get(key); found {
			if value == "" {
				return "", false // Tombstone (deleted)
			}
			return value, true
		}
	}

	// Check SSTables (newest to oldest)
	for _, sst := range db.sstables {
		if value, found, err := sst.Get(key); err == nil && found {
			if value == "" {
				return "", false // Tombstone (deleted)
			}
			return value, true
		}
	}

	return "", false
}

func (db *DB) Delete(key string) error {
	return db.Set(key, "") // Tombstone
}

func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		db.closed = true

		// Close current memtable
		if err = db.currMemtable.Close(); err != nil {
			err = fmt.Errorf("failed to close current memtable: %w", err)
			return
		}

		// Close all immutable memtables
		for _, mt := range db.immutable {
			if e := mt.Close(); e != nil {
				err = fmt.Errorf("failed to close immutable memtable: %w", e)
				return
			}
		}

		// Close all SSTables
		for _, sst := range db.sstables {
			if e := sst.Close(); e != nil {
				err = fmt.Errorf("failed to close SSTable: %w", e)
				return
			}
		}
	})
	return err
}

func (db *DB) flushImmutableMemtables() {
	db.mu.Lock()
	mts := make([]*Memtable, len(db.immutable))
	copy(mts, db.immutable)
	db.mu.Unlock()

	var newSSTables []*SSTable
	var flushedMts []*Memtable

	for _, mt := range mts {
		// Generate unique chronological filename
		filename := fmt.Sprintf("%020d.sst", time.Now().UnixNano())
		sstPath := filepath.Join(db.sstDir, filename)

		// Write sorted data (Slow I/O, done without database lock)
		iterator := mt.data.Iterator()
		if err := WriteSSTable(sstPath, iterator); err != nil {
			fmt.Printf("failed to write SSTable: %v\n", err)
			continue
		}

		// Open SSTable (Slow I/O, done without database lock)
		sst, err := OpenSSTable(sstPath)
		if err != nil {
			fmt.Printf("failed to open written SSTable: %v\n", err)
			continue
		}

		newSSTables = append(newSSTables, sst)
		flushedMts = append(flushedMts, mt)
	}

	if len(newSSTables) == 0 {
		return
	}

	db.mu.Lock()
	// Prepend new SSTables to db.sstables (newest first)
	for i := len(newSSTables) - 1; i >= 0; i-- {
		db.sstables = append([]*SSTable{newSSTables[i]}, db.sstables...)
	}

	// Remove flushed memtables from immutable list
	var remaining []*Memtable
	for _, mt := range db.immutable {
		flushed := false
		for _, fmt := range flushedMts {
			if mt == fmt {
				flushed = true
				break
			}
		}
		if !flushed {
			remaining = append(remaining, mt)
		}
	}
	db.immutable = remaining
	db.mu.Unlock()

	// Clean up WAL files and memory for flushed memtables
	go func() {
		for _, mt := range flushedMts {
			mt.Close()
			walPath := mt.wal.File.Name()
			if err := os.Remove(walPath); err != nil {
				fmt.Printf("failed to remove WAL file %s: %v\n", walPath, err)
			}
		}
	}()

	// Trigger compaction if we exceeded threshold
	db.triggerCompaction()
}

func (db *DB) triggerCompaction() {
	db.mu.Lock()
	if db.closed || len(db.sstables) < db.options.CompactionThreshold {
		db.mu.Unlock()
		return
	}
	db.mu.Unlock()

	go db.runCompaction()
}

func (db *DB) runCompaction() {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return
	}
	// Copy sstables to compact
	sstsToCompact := make([]*SSTable, len(db.sstables))
	copy(sstsToCompact, db.sstables)
	db.mu.Unlock()

	if len(sstsToCompact) < 2 {
		return
	}

	// Create compacted file path
	filename := fmt.Sprintf("%020d_compact.sst", time.Now().UnixNano())
	destPath := filepath.Join(db.sstDir, filename)

	err := mergeSSTables(destPath, sstsToCompact)
	if err != nil {
		fmt.Printf("compaction failed: %v\n", err)
		return
	}

	newSst, err := OpenSSTable(destPath)
	if err != nil {
		fmt.Printf("failed to open compacted SSTable: %v\n", err)
		return
	}

	db.mu.Lock()
	if db.closed {
		newSst.Close()
		os.Remove(destPath)
		db.mu.Unlock()
		return
	}

	// Filter out the compacted SSTables from active list (keeping new ones created during compaction)
	var updatedSSTables []*SSTable
	for _, sst := range db.sstables {
		compacted := false
		for _, csst := range sstsToCompact {
			if sst.filePath == csst.filePath {
				compacted = true
				break
			}
		}
		if !compacted {
			updatedSSTables = append(updatedSSTables, sst)
		}
	}

	// Append the compacted SSTable to the end (representing older combined data)
	db.sstables = append(updatedSSTables, newSst)
	db.mu.Unlock()

	// Close and delete compacted files
	go func() {
		for _, sst := range sstsToCompact {
			sst.Close()
			if err := os.Remove(sst.filePath); err != nil {
				fmt.Printf("failed to remove old SSTable %s: %v\n", sst.filePath, err)
			}
		}
	}()
}

func mergeSSTables(destPath string, sstables []*SSTable) error {
	if len(sstables) == 0 {
		return nil
	}

	iterators := make([]*SSTableIterator, len(sstables))
	for i, sst := range sstables {
		iterators[i] = sst.Iterator()
		iterators[i].Next() // Prime iterator
	}

	tempPath := destPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	defer os.Remove(tempPath)

	var index []IndexEntry
	var offset int64

	for {
		smallestIdx := -1
		var smallestKey string

		for i, it := range iterators {
			if it.currIdx >= len(it.sst.index) {
				continue
			}

			key := it.Key()
			if smallestIdx == -1 || key < smallestKey {
				smallestIdx = i
				smallestKey = key
			} else if key == smallestKey {
				// i is newer because sstables slice is ordered newest-first
				if i < smallestIdx {
					iterators[smallestIdx].Next()
					smallestIdx = i
					smallestKey = key
				} else {
					it.Next()
				}
			}
		}

		if smallestIdx == -1 {
			break
		}

		it := iterators[smallestIdx]
		key := it.Key()
		val := it.Value()
		it.Next()

		// Reclaim space by discarding tombstones during compaction
		if val == "" {
			continue
		}

		keyLen := uint32(len(key))
		valLen := uint32(len(val))

		var header [8]byte
		binary.BigEndian.PutUint32(header[0:4], keyLen)
		binary.BigEndian.PutUint32(header[4:8], valLen)

		if _, err := file.Write(header[:]); err != nil {
			return err
		}

		index = append(index, IndexEntry{
			Key:    key,
			Offset: offset,
			ValLen: valLen,
		})

		if _, err := file.WriteString(key); err != nil {
			return err
		}
		if _, err := file.WriteString(val); err != nil {
			return err
		}

		offset += 8 + int64(keyLen) + int64(valLen)
	}

	// Write index block
	indexOffset := offset
	for _, entry := range index {
		keyLen := uint32(len(entry.Key))
		var idxHeader [16]byte
		binary.BigEndian.PutUint32(idxHeader[0:4], keyLen)
		binary.BigEndian.PutUint64(idxHeader[4:12], uint64(entry.Offset))
		binary.BigEndian.PutUint32(idxHeader[12:16], entry.ValLen)

		if _, err := file.Write(idxHeader[:]); err != nil {
			return err
		}
		if _, err := file.WriteString(entry.Key); err != nil {
			return err
		}
		offset += 16 + int64(keyLen)
	}

	// Write Footer
	var footer [16]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint32(footer[8:12], uint32(len(index)))
	binary.BigEndian.PutUint32(footer[12:16], MagicNumber)

	if _, err := file.Write(footer[:]); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}
	file.Close()

	return os.Rename(tempPath, destPath)
}

func (db *DB) Scan(prefix string) (map[string]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDBClosed
	}

	results := make(map[string]string)

	// 1. Scan SSTables from oldest to newest (so newer ones overwrite older ones)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]
		idx := sort.Search(len(sst.index), func(j int) bool {
			return sst.index[j].Key >= prefix
		})

		for j := idx; j < len(sst.index); j++ {
			entry := sst.index[j]
			if !strings.HasPrefix(entry.Key, prefix) {
				break
			}
			valOffset := entry.Offset + 8 + int64(len(entry.Key))
			valBuf := make([]byte, entry.ValLen)
			if _, err := sst.file.ReadAt(valBuf, valOffset); err != nil {
				return nil, fmt.Errorf("failed to read value during scan: %w", err)
			}
			results[entry.Key] = string(valBuf)
		}
	}

	// 2. Scan immutable memtables (oldest to newest)
	for i := 0; i < len(db.immutable); i++ {
		mt := db.immutable[i]
		it := mt.data.Iterator()
		for it.Next() {
			key := it.Key()
			if strings.HasPrefix(key, prefix) {
				results[key] = it.Value()
			}
		}
	}

	// 3. Scan active memtable
	it := db.currMemtable.data.Iterator()
	for it.Next() {
		key := it.Key()
		if strings.HasPrefix(key, prefix) {
			results[key] = it.Value()
		}
	}

	// 4. Remove tombstones (empty values)
	for k, v := range results {
		if v == "" {
			delete(results, k)
		}
	}

	return results, nil
}


