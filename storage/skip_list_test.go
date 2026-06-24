package storage

import (
	"testing"
)

func TestSkipListPutAndGet(t *testing.T) {
	sl := InitSL(0.5, 16)

	// Test basic insertions
	sl.Put("key1", "val1")
	sl.Put("key3", "val3")
	sl.Put("key2", "val2")

	if v, ok := sl.Get("key1"); !ok || v != "val1" {
		t.Errorf("expected val1, got %v (ok=%v)", v, ok)
	}
	if v, ok := sl.Get("key2"); !ok || v != "val2" {
		t.Errorf("expected val2, got %v (ok=%v)", v, ok)
	}
	if v, ok := sl.Get("key3"); !ok || v != "val3" {
		t.Errorf("expected val3, got %v (ok=%v)", v, ok)
	}

	// Test duplicate key updates
	sl.Put("key2", "new_val2")
	if v, ok := sl.Get("key2"); !ok || v != "new_val2" {
		t.Errorf("expected updated new_val2, got %v (ok=%v)", v, ok)
	}

	// Make sure size is correct
	if sl.Size() != 3 {
		t.Errorf("expected size 3, got %d", sl.Size())
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := InitSL(0.5, 16)

	sl.Put("key1", "val1")
	sl.Put("key2", "val2")

	if deleted := sl.Delete("key1"); !deleted {
		t.Errorf("expected key1 to be deleted")
	}

	if _, ok := sl.Get("key1"); ok {
		t.Errorf("expected key1 to be missing")
	}

	if sl.Size() != 1 {
		t.Errorf("expected size 1, got %d", sl.Size())
	}
}
