package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type DB struct {
	Name      string
	store     *Memtable
	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	options   Options
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
		return nil, errors.New("db name cannot be empty")
	}

	dbPath := filepath.Join(opts.DataDir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	store, err := InitMemtable(dbPath, opts.MemtableSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize memtable: %w", err)
	}

	return &DB{
		Name:    name,
		store:   store,
		options: opts,
	}, nil
}

func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		db.closed = true
		err = db.store.Close()
	})
	return err
}

func (db *DB) Set(key string, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.store.Set(key, value); err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.store.Get(key)
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.store.Delete(key)
}
