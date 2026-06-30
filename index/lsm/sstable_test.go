package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWriteAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sst_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sstPath := filepath.Join(tmpDir, "00001.sst")

	// 1. Create a skip list and fill it
	sl := InitSL(0.5, 16)
	sl.Put("apple", "red")
	sl.Put("banana", "yellow")
	sl.Put("cherry", "dark red")

	// 2. Write to SSTable
	iterator := sl.Iterator()
	err = WriteSSTable(sstPath, iterator)
	if err != nil {
		t.Fatalf("failed to write SSTable: %v", err)
	}

	// 3. Open SSTable
	sst, err := OpenSSTable(sstPath)
	if err != nil {
		t.Fatalf("failed to open SSTable: %v", err)
	}
	defer sst.Close()

	// 4. Test lookups
	testCases := []struct {
		key      string
		expected string
		found    bool
	}{
		{"apple", "red", true},
		{"banana", "yellow", true},
		{"cherry", "dark red", true},
		{"durian", "", false},
	}

	for _, tc := range testCases {
		val, ok, err := sst.Get(tc.key)
		if err != nil {
			t.Errorf("Get error for key %s: %v", tc.key, err)
		}
		if ok != tc.found {
			t.Errorf("key %s: expected found=%v, got %v", tc.key, tc.found, ok)
		}
		if val != tc.expected {
			t.Errorf("key %s: expected %q, got %q", tc.key, tc.expected, val)
		}
	}

	// 5. Test Iterator
	sstIt := sst.Iterator()
	expectedKeys := []string{"apple", "banana", "cherry"}
	expectedVals := []string{"red", "yellow", "dark red"}
	idx := 0
	for sstIt.Next() {
		if sstIt.Key() != expectedKeys[idx] {
			t.Errorf("expected key %s, got %s", expectedKeys[idx], sstIt.Key())
		}
		if sstIt.Value() != expectedVals[idx] {
			t.Errorf("expected val %s, got %s", expectedVals[idx], sstIt.Value())
		}
		idx++
	}
	if sstIt.Error() != nil {
		t.Errorf("iterator error: %v", sstIt.Error())
	}
	if idx != 3 {
		t.Errorf("expected iterator to process 3 items, got %d", idx)
	}
}
