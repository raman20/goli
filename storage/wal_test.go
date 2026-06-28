package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_tx_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// 1. Initialize and write transactions
	wal, err := InitWal(walPath)
	if err != nil {
		t.Fatalf("failed to init WAL: %v", err)
	}

	// Tx 1: Committed
	wal.WriteTxStart(1)
	wal.WriteTxSet(1, "key1", "val1")
	wal.WriteTxSet(1, "key2", "val2")
	wal.WriteTxCommit(1)

	// Tx 2: Uncommitted (simulates crash before commit)
	wal.WriteTxStart(2)
	wal.WriteTxSet(2, "key3", "val3")
	wal.WriteTxDelete(2, "key1")

	// Tx 3: Committed
	wal.WriteTxStart(3)
	wal.WriteTxSet(3, "key4", "val4")
	wal.WriteTxDelete(3, "key2")
	wal.WriteTxCommit(3)

	wal.Close()

	// 2. Open and replay WAL
	wal2, err := InitWal(walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	ops, err := wal2.Read()
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}

	// Verify only committed transactions (Tx 1 and Tx 3) are returned
	expectedOps := []RecoveredOp{
		{Key: "key1", Value: "val1", Delete: false},
		{Key: "key2", Value: "val2", Delete: false},
		{Key: "key4", Value: "val4", Delete: false},
		{Key: "key2", Value: "", Delete: true},
	}

	if len(ops) != len(expectedOps) {
		t.Fatalf("expected %d operations, got %d", len(expectedOps), len(ops))
	}

	for i, op := range ops {
		expected := expectedOps[i]
		if op.Key != expected.Key || op.Value != expected.Value || op.Delete != expected.Delete {
			t.Errorf("op %d: expected %+v, got %+v", i, expected, op)
		}
	}
}
