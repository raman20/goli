package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriteAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// 1. Write entries
	wal, err := InitWal(walPath)
	if err != nil {
		t.Fatalf("failed to init WAL: %v", err)
	}

	testData := map[string]string{
		"key1":             "value1",
		"key:with:colon":   "value:with:colon",
		"key_with_newline": "value\nwith\nnewline",
		"binary_data":      "\x00\x01\x02\x03\xff",
	}

	for k, v := range testData {
		if err := wal.Entry(k, v); err != nil {
			t.Fatalf("failed to write entry %q: %v", k, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// 2. Read entries back
	wal2, err := InitWal(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL for reading: %v", err)
	}
	defer wal2.Close()

	recovered, err := wal2.Read()
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}

	if len(recovered) != len(testData) {
		t.Errorf("expected %d entries, got %d", len(testData), len(recovered))
	}

	for k, expectedVal := range testData {
		gotVal, ok := recovered[k]
		if !ok {
			t.Errorf("key %q was not recovered", k)
			continue
		}
		if gotVal != expectedVal {
			t.Errorf("for key %q: expected %q, got %q", k, expectedVal, gotVal)
		}
	}
}
