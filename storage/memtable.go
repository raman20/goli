package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type Memtable struct {
	id        string
	wal       *WAL
	data      *SkipList
	mu        sync.RWMutex
	size      int64 // Track size for flush decisions
	maxSize   int64 // Maximum size before flush
	closed    bool  // Track if memtable is closed
	closeOnce sync.Once
}

func InitMemtable(dbName string, maxSize int64) (*Memtable, error) {
	if maxSize <= 0 {
		maxSize = 32 * 1024 * 1024 // Default 32MB
	}

	id := uuid.NewString()
	walPath := filepath.Join(dbName, id+"_wal.log")

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
	for k, v := range entries {
		skl.Put(k, v)
	}

	return &Memtable{
		id:      id,
		wal:     wal,
		data:    skl,
		maxSize: maxSize,
	}, nil
}

func (m *Memtable) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("memtable is closed")
	}

	// Estimate size increase
	newSize := m.size + int64(len(key)+len(value))
	if newSize > m.maxSize {
		return errors.New("memtable is full")
	}

	if err := m.wal.Entry(key, value); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
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
	return m.Set(key, "") // Tombstone
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

// func Flush()  {}
