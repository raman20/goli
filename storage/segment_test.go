package storage

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestSegmentManagerBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set small maxSize to force rollovers quickly
	sm, err := NewSegmentManager(tmpDir, 100)
	if err != nil {
		t.Fatalf("failed to create SegmentManager: %v", err)
	}

	// 1. Write data that fits in Segment 1
	data1 := []byte("hello world")
	ref1, err := sm.Append(data1)
	if err != nil {
		t.Fatalf("failed to append data1: %v", err)
	}
	if ref1.FileID != 1 || ref1.Offset != 0 || ref1.Length != uint32(len(data1)) {
		t.Errorf("unexpected ref1: %+v", ref1)
	}

	// 2. Write data that triggers rollover to Segment 2
	data2 := make([]byte, 120) // Exceeds 100-byte limits
	ref2, err := sm.Append(data2)
	if err != nil {
		t.Fatalf("failed to append data2: %v", err)
	}
	if ref2.FileID != 2 || ref2.Offset != 0 || ref2.Length != 120 {
		t.Errorf("unexpected ref2: %+v", ref2)
	}

	// 3. Write data (will trigger rollover to Segment 3 because Segment 2 is already at 120 bytes, exceeding the 100-byte limit)
	data3 := []byte("extra bytes")
	ref3, err := sm.Append(data3)
	if err != nil {
		t.Fatalf("failed to append data3: %v", err)
	}
	if ref3.FileID != 3 || ref3.Offset != 0 || ref3.Length != uint32(len(data3)) {
		t.Errorf("unexpected ref3: %+v", ref3)
	}

	// 4. Verify Reads
	res1, err := sm.Read(ref1)
	if err != nil {
		t.Fatalf("failed to read ref1: %v", err)
	}
	if !bytes.Equal(res1, data1) {
		t.Errorf("expected %q, got %q", data1, res1)
	}

	res3, err := sm.Read(ref3)
	if err != nil {
		t.Fatalf("failed to read ref3: %v", err)
	}
	if !bytes.Equal(res3, data3) {
		t.Errorf("expected %q, got %q", data3, res3)
	}

	sm.Close()
}

func TestSegmentManagerConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment_test_concurrent")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewSegmentManager(tmpDir, 4096)
	if err != nil {
		t.Fatalf("failed to create SegmentManager: %v", err)
	}
	defer sm.Close()

	var wg sync.WaitGroup
	numWorkers := 10
	numAppends := 50
	refs := make([][]RecordRef, numWorkers)

	// Concurrent writes
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			refs[workerId] = make([]RecordRef, numAppends)
			for j := 0; j < numAppends; j++ {
				data := []byte(fmt.Sprintf("worker-%d-append-%d", workerId, j))
				ref, err := sm.Append(data)
				if err != nil {
					t.Errorf("failed concurrent append: %v", err)
					return
				}
				refs[workerId][j] = ref
			}
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for j := 0; j < numAppends; j++ {
				ref := refs[workerId][j]
				res, err := sm.Read(ref)
				if err != nil {
					t.Errorf("failed concurrent read: %v", err)
					return
				}
				expected := []byte(fmt.Sprintf("worker-%d-append-%d", workerId, j))
				if !bytes.Equal(res, expected) {
					t.Errorf("expected %q, got %q", expected, res)
				}
			}
		}(i)
	}
	wg.Wait()
}
