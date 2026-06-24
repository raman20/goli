package storage

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDBEndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		MemtableSize:        1024, // Small size to force rapid rotation
		DataDir:             tmpDir,
		CompactionThreshold: 3,
	}

	// 1. Open Database
	db, err := Open("test_db", opts)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}

	// 2. Put a bunch of keys to trigger rotations
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%03d", i)
		val := fmt.Sprintf("value_data_long_string_to_fill_memtable_%03d", i)
		if err := db.Set(key, val); err != nil {
			t.Fatalf("failed to set key %s: %v", key, err)
		}
	}

	// Wait slightly for background flushes to complete
	time.Sleep(100 * time.Millisecond)

	// Verify all keys are readable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%03d", i)
		expectedVal := fmt.Sprintf("value_data_long_string_to_fill_memtable_%03d", i)
		val, ok := db.Get(key)
		if !ok {
			t.Errorf("expected key %s to be found", key)
		}
		if val != expectedVal {
			t.Errorf("key %s: expected %q, got %q", key, expectedVal, val)
		}
	}

	// 3. Test Deletes / Tombstones
	deleteKey := "key_050"
	if err := db.Delete(deleteKey); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	if _, ok := db.Get(deleteKey); ok {
		t.Errorf("key %s was deleted but Get returned found=true", deleteKey)
	}

	// 4. Test crash recovery
	// Close and reopen db to test recovery from WAL and loaded SSTs
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	db2, err := Open("test_db", opts)
	if err != nil {
		t.Fatalf("failed to reopen DB: %v", err)
	}
	defer db2.Close()

	// The deleted key should still be missing
	if _, ok := db2.Get(deleteKey); ok {
		t.Errorf("key %s was deleted but after reopen Get returned found=true", deleteKey)
	}

	// Check another key to ensure recovery works
	verifyKey := "key_075"
	expectedVal := "value_data_long_string_to_fill_memtable_075"
	if val, ok := db2.Get(verifyKey); !ok || val != expectedVal {
		t.Errorf("key %s: expected %q, got %q (found=%v) after recovery", verifyKey, expectedVal, val, ok)
	}
}

func TestDBScan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db_scan_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := Options{
		MemtableSize: 1024 * 1024,
		DataDir:      tmpDir,
	}

	db, err := Open("test_scan_db", opts)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	// Set some keys with prefixes
	db.Set("user:1:name", "Alice")
	db.Set("user:1:age", "30")
	db.Set("user:2:name", "Bob")
	db.Set("group:1:name", "Admins")

	// Scan by prefix
	results, err := db.Scan("user:")
	if err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	expected := map[string]string{
		"user:1:name": "Alice",
		"user:1:age":  "30",
		"user:2:name": "Bob",
	}

	if len(results) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(results))
	}

	for k, expectedVal := range expected {
		gotVal, ok := results[k]
		if !ok || gotVal != expectedVal {
			t.Errorf("for key %s: expected %q, got %q", k, expectedVal, gotVal)
		}
	}

	// Delete a key and check that scan excludes it
	db.Delete("user:1:age")
	results2, err := db.Scan("user:")
	if err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if _, ok := results2["user:1:age"]; ok {
		t.Errorf("expected deleted key user:1:age to be excluded from scan")
	}
}

