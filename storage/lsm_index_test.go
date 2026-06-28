package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLSMIndexBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lsm_index_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walDir := filepath.Join(tmpDir, "wal")
	sstDir := filepath.Join(tmpDir, "sst")
	os.MkdirAll(walDir, 0755)
	os.MkdirAll(sstDir, 0755)

	opts := Options{
		MemtableSize:        256, // Small size to force rapid rotation
		CompactionThreshold: 2,
	}

	// 1. Initialize Index
	idx, err := NewLSMIndex(walDir, sstDir, opts, nil)
	if err != nil {
		t.Fatalf("failed to create LSMIndex: %v", err)
	}

	// 2. Put key coordinates
	ref1 := RecordRef{FileID: 1, Offset: 100, Length: 20}
	if err := idx.Put([]byte("key1"), ref1); err != nil {
		t.Fatalf("failed to put ref1: %v", err)
	}

	ref2 := RecordRef{FileID: 1, Offset: 120, Length: 30}
	if err := idx.Put([]byte("key2"), ref2); err != nil {
		t.Fatalf("failed to put ref2: %v", err)
	}

	// 3. Get key coordinates
	gotRef, found, err := idx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found || gotRef != ref1 {
		t.Errorf("expected found=true and %+v, got found=%v and %+v", ref1, found, gotRef)
	}

	// 4. Test rotation and rollover to SSTable
	// Set more keys to fill memtable
	for i := 0; i < 20; i++ {
		ref := RecordRef{FileID: 2, Offset: int64(i * 10), Length: 10}
		idx.Put([]byte(string(rune(i+65))), ref)
	}

	// Wait slightly for background flush
	time.Sleep(100 * time.Millisecond)

	// Retrieve from SSTable
	gotRef, found, err = idx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found || gotRef != ref1 {
		t.Errorf("expected found=true and %+v from SSTable, got found=%v and %+v", ref1, found, gotRef)
	}

	// 5. Test Delete (Tombstone)
	if err := idx.Delete([]byte("key1")); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, found, err = idx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get error after delete: %v", err)
	}
	if found {
		t.Errorf("expected key1 to be not found (tombstone active)")
	}

	idx.Close()
}
