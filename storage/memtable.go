package storage

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrMemtableFull   = errors.New("memtable is full")
	ErrMemtableClosed = errors.New("memtable is closed")
)

type Memtable struct {
	wal       *WAL
	data      *SkipList
	mu        sync.RWMutex
	size      int64 // Track size for flush decisions
	maxSize   int64 // Maximum size before flush
	closed    bool  // Track if memtable is closed
	closeOnce sync.Once
}

func InitMemtable(walPath string, maxSize int64) (*Memtable, error) {
	if maxSize <= 0 {
		maxSize = 32 * 1024 * 1024 // Default 32MB
	}

	wal, err := InitWal(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	// Recover from WAL if it exists
	entries, err := wal.Read()
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	skl := InitSL(0.5, 16)

	// Replay WAL entries
	for _, op := range entries {
		if op.Delete {
			skl.Put(op.Key, "") // Replay delete as tombstone
		} else {
			skl.Put(op.Key, op.Value)
		}
	}

	return &Memtable{
		wal:     wal,
		data:    skl,
		maxSize: maxSize,
	}, nil
}

func (m *Memtable) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrMemtableClosed
	}

	// Estimate size increase
	newSize := m.size + int64(len(key)+len(value))
	if newSize > m.maxSize {
		return ErrMemtableFull
	}

	if err := m.wal.WriteTxStart(0); err != nil {
		return fmt.Errorf("failed to write WAL start: %w", err)
	}
	if err := m.wal.WriteTxSet(0, key, value); err != nil {
		return fmt.Errorf("failed to write WAL set: %w", err)
	}
	if err := m.wal.WriteTxCommit(0); err != nil {
		return fmt.Errorf("failed to write WAL commit: %w", err)
	}

	m.data.Put(key, value)
	m.size = newSize

	return nil
}

func (m *Memtable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return "", false
	}

	return m.data.Get(key)
}

func (m *Memtable) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrMemtableClosed
	}

	newSize := m.size + int64(len(key))
	if newSize > m.maxSize {
		return ErrMemtableFull
	}

	if err := m.wal.WriteTxStart(0); err != nil {
		return fmt.Errorf("failed to write WAL start: %w", err)
	}
	if err := m.wal.WriteTxDelete(0, key); err != nil {
		return fmt.Errorf("failed to write WAL delete: %w", err)
	}
	if err := m.wal.WriteTxCommit(0); err != nil {
		return fmt.Errorf("failed to write WAL commit: %w", err)
	}

	m.data.Put(key, "") // Put empty string tombstone
	m.size = newSize

	return nil
}

func (m *Memtable) Close() error {
	var err error
	m.closeOnce.Do(func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.closed = true
		err = m.wal.Close()
	})
	return err
}

// Size returns the current size of the memtable
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Memtable) DataBlock() *SkipList {
	return m.data
}

func (m *Memtable) WALFile() *WAL {
	return m.wal
}
