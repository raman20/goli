package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

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
	mu           sync.RWMutex
	closed       bool
	closeOnce    sync.Once
	options      Options
	walDir       string
	sstDir       string
	// sstables     []*SSTable
}

type Options struct {
	MemtableSize   int64
	MaxConcurrency int
	SyncWrites     bool
	DataDir        string
}

func DefaultOptions() Options {
	return Options{
		MemtableSize:   32 * 1024 * 1024, // 32MB
		MaxConcurrency: runtime.GOMAXPROCS(0),
		SyncWrites:     true,
		DataDir:        "data",
	}
}

func Open(name string, opts Options) (*DB, error) {
	if name == "" {
		return nil, ErrKeyEmpty
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

	// Create new active memtable
	if err := db.rotateMemtable(); err != nil {
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
		return value, true
	}

	// Then check all immutable memtables
	for _, mt := range db.immutable {
		if value, found := mt.Get(key); found {
			return value, true
		}
	}

	// TODO: Check SSTable index for older data
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
	})
	return err
}

func (db *DB) flushImmutableMemtables() {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, mt := range db.immutable {
		if err := db.compactMemtable(mt); err != nil {
			fmt.Printf("failed to compact memtable: %v\n", err)
			continue
		}

		// Remove WAL file
		walPath := mt.wal.File.Name()
		if err := os.Remove(walPath); err != nil {
			fmt.Printf("failed to remove WAL file %s: %v\n", walPath, err)
		}
	}

	// Clear the immutable slice after flushing
	db.immutable = nil
}

func (db *DB) compactMemtable(mt *Memtable) error {
	// TODO: Implement SSTable creation
	// 1. Create new SSTable file
	// 2. Write sorted key-value pairs
	// 3. Create index blocks
	// 4. Write metadata
	// 5. Sync file
	return nil
}
