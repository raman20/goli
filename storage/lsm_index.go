package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"
)

type LSMIndex struct {
	currMemtable *Memtable
	immutable    []*Memtable
	sstables     []*SSTable
	mu           sync.RWMutex
	options      Options
	walDir       string
	sstDir       string
	closed       bool
}

// NewLSMIndex initializes the LSM-Tree index.
func NewLSMIndex(walDir, sstDir string, opts Options, sstables []*SSTable) (*LSMIndex, error) {
	idx := &LSMIndex{
		walDir:   walDir,
		sstDir:   sstDir,
		options:  opts,
		sstables: sstables,
	}

	// Recover existing WAL files as immutable memtables
	wals, err := os.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	for _, wal := range wals {
		if !wal.IsDir() {
			mt, err := InitMemtable(filepath.Join(walDir, wal.Name()), opts.MemtableSize)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize memtable from WAL %s: %w", wal.Name(), err)
			}
			idx.immutable = append(idx.immutable, mt)
		}
	}

	// Create initial active memtable
	if err := idx.rotateMemtable(); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *LSMIndex) rotateMemtable() error {
	walFile := filepath.Join(idx.walDir, uuid.NewString()+".log")
	newMemtable, err := InitMemtable(walFile, idx.options.MemtableSize)
	if err != nil {
		return fmt.Errorf("failed to create active memtable: %w", err)
	}

	if idx.currMemtable != nil {
		idx.immutable = append(idx.immutable, idx.currMemtable)
		go idx.flushImmutableMemtables()
	}

	idx.currMemtable = newMemtable
	return nil
}

func marshalRef(ref RecordRef) string {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], ref.FileID)
	binary.BigEndian.PutUint64(buf[4:12], uint64(ref.Offset))
	binary.BigEndian.PutUint32(buf[12:16], ref.Length)
	return unsafe.String(&buf[0], 16)
}

func unmarshalRef(data string) RecordRef {
	if len(data) < 16 {
		return RecordRef{}
	}
	fileID := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	offset := int64(data[4])<<56 | int64(data[5])<<48 | int64(data[6])<<40 | int64(data[7])<<32 |
		int64(data[8])<<24 | int64(data[9])<<16 | int64(data[10])<<8 | int64(data[11])
	length := uint32(data[12])<<24 | uint32(data[13])<<16 | uint32(data[14])<<8 | uint32(data[15])
	return RecordRef{
		FileID: fileID,
		Offset: offset,
		Length: length,
	}
}

// Put maps a key to its RecordRef in the active memtable.
func (idx *LSMIndex) Put(key []byte, ref RecordRef) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return ErrDBClosed
	}

	valStr := marshalRef(ref)
	err := idx.currMemtable.Set(string(key), valStr)
	if err != nil {
		if errors.Is(err, ErrMemtableFull) {
			if err := idx.rotateMemtable(); err != nil {
				return err
			}
			return idx.currMemtable.Set(string(key), valStr)
		}
		return err
	}

	return nil
}

// Get retrieves the RecordRef for a key by searching the memtables and SSTables.
func (idx *LSMIndex) Get(key []byte) (RecordRef, bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return RecordRef{}, false, ErrDBClosed
	}

	keyStr := string(key)

	// 1. Check active memtable
	if value, found := idx.currMemtable.Get(keyStr); found {
		if value == "" {
			return RecordRef{}, false, nil // Tombstone
		}
		return unmarshalRef(value), true, nil
	}

	// 2. Check immutable memtables (newest to oldest)
	for i := len(idx.immutable) - 1; i >= 0; i-- {
		mt := idx.immutable[i]
		if value, found := mt.Get(keyStr); found {
			if value == "" {
				return RecordRef{}, false, nil // Tombstone
			}
			return unmarshalRef(value), true, nil
		}
	}

	// 3. Check SSTables (newest to oldest)
	for _, sst := range idx.sstables {
		if value, found, err := sst.Get(keyStr); err == nil && found {
			if value == "" {
				return RecordRef{}, false, nil // Tombstone
			}
			return unmarshalRef(value), true, nil
		}
	}

	return RecordRef{}, false, nil
}

// Delete writes a tombstone for a key.
func (idx *LSMIndex) Delete(key []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return ErrDBClosed
	}

	err := idx.currMemtable.Delete(string(key))
	if err != nil {
		if errors.Is(err, ErrMemtableFull) {
			if err := idx.rotateMemtable(); err != nil {
				return err
			}
			return idx.currMemtable.Delete(string(key))
		}
		return err
	}

	return nil
}

// Scan returns all RecordRefs whose keys start with the prefix.
func (idx *LSMIndex) Scan(prefix []byte) ([]RecordRef, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, ErrDBClosed
	}

	prefixStr := string(prefix)
	results := make(map[string]string)

	// 1. Scan SSTables (oldest to newest)
	for i := len(idx.sstables) - 1; i >= 0; i-- {
		sst := idx.sstables[i]
		pos := sortSearchKeys(sst.index, prefixStr)

		for j := pos; j < len(sst.index); j++ {
			entry := sst.index[j]
			if !strings.HasPrefix(entry.Key, prefixStr) {
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

	// 2. Scan immutable memtables
	for i := 0; i < len(idx.immutable); i++ {
		mt := idx.immutable[i]
		it := mt.data.Iterator()
		for it.Next() {
			key := it.Key()
			if strings.HasPrefix(key, prefixStr) {
				results[key] = it.Value()
			}
		}
	}

	// 3. Scan active memtable
	it := idx.currMemtable.data.Iterator()
	for it.Next() {
		key := it.Key()
		if strings.HasPrefix(key, prefixStr) {
			results[key] = it.Value()
		}
	}

	// 4. Resolve references and filter tombstones
	var refs []RecordRef
	for _, v := range results {
		if v != "" {
			refs = append(refs, unmarshalRef(v))
		}
	}

	return refs, nil
}

func sortSearchKeys(index []IndexEntry, prefix string) int {
	low, high := 0, len(index)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if index[mid].Key >= prefix {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

func (idx *LSMIndex) flushImmutableMemtables() {
	idx.mu.Lock()
	mts := make([]*Memtable, len(idx.immutable))
	copy(mts, idx.immutable)
	idx.mu.Unlock()

	var newSSTables []*SSTable
	var flushedMts []*Memtable

	for _, mt := range mts {
		filename := fmt.Sprintf("%020d.sst", time.Now().UnixNano())
		sstPath := filepath.Join(idx.sstDir, filename)

		iterator := mt.data.Iterator()
		if err := WriteSSTable(sstPath, iterator); err != nil {
			fmt.Printf("failed to write SSTable: %v\n", err)
			continue
		}

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

	idx.mu.Lock()
	for i := len(newSSTables) - 1; i >= 0; i-- {
		idx.sstables = append([]*SSTable{newSSTables[i]}, idx.sstables...)
	}

	var remaining []*Memtable
	for _, mt := range idx.immutable {
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
	idx.immutable = remaining
	idx.mu.Unlock()

	go func() {
		for _, mt := range flushedMts {
			mt.Close()
			walPath := mt.wal.File.Name()
			if err := os.Remove(walPath); err != nil {
				fmt.Printf("failed to remove WAL file %s: %v\n", walPath, err)
			}
		}
	}()

	idx.triggerCompaction()
}

func (idx *LSMIndex) triggerCompaction() {
	idx.mu.Lock()
	if idx.closed || len(idx.sstables) < idx.options.CompactionThreshold {
		idx.mu.Unlock()
		return
	}
	idx.mu.Unlock()

	go idx.runCompaction()
}

func (idx *LSMIndex) runCompaction() {
	idx.mu.Lock()
	if idx.closed {
		idx.mu.Unlock()
		return
	}
	sstsToCompact := make([]*SSTable, len(idx.sstables))
	copy(sstsToCompact, idx.sstables)
	idx.mu.Unlock()

	if len(sstsToCompact) < 2 {
		return
	}

	filename := fmt.Sprintf("%020d_compact.sst", time.Now().UnixNano())
	destPath := filepath.Join(idx.sstDir, filename)

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

	idx.mu.Lock()
	if idx.closed {
		newSst.Close()
		os.Remove(destPath)
		idx.mu.Unlock()
		return
	}

	var updatedSSTables []*SSTable
	for _, sst := range idx.sstables {
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

	idx.sstables = append(updatedSSTables, newSst)
	idx.mu.Unlock()

	go func() {
		for _, sst := range sstsToCompact {
			sst.Close()
			if err := os.Remove(sst.filePath); err != nil {
				fmt.Printf("failed to remove old SSTable %s: %v\n", sst.filePath, err)
			}
		}
	}()
}

// Close closes all open resources (active/immutable memtables and SSTables).
func (idx *LSMIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}
	idx.closed = true

	var firstErr error
	if err := idx.currMemtable.Close(); err != nil {
		firstErr = err
	}

	for _, mt := range idx.immutable {
		if err := mt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, sst := range idx.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
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
