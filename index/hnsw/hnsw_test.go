package hnsw

import (
	"fmt"
	"testing"

	"github.com/raman20/storage"
)

func TestHNSWKeyEncoding(t *testing.T) {
	id := "item_123"
	vector := []float32{0.15, -0.4, 0.9, 0.0, 1.25}

	key := EncodeKey(id, vector)
	decodedID, decodedVector, err := DecodeKey(key)
	if err != nil {
		t.Fatalf("failed to decode key: %v", err)
	}

	if decodedID != id {
		t.Errorf("expected ID %q, got %q", id, decodedID)
	}

	if len(decodedVector) != len(vector) {
		t.Fatalf("expected vector length %d, got %d", len(vector), len(decodedVector))
	}

	for i := range vector {
		if decodedVector[i] != vector[i] {
			t.Errorf("index %d: expected %f, got %f", i, vector[i], decodedVector[i])
		}
	}
}

func TestHNSWBasicSearch(t *testing.T) {
	// Initialize HNSW index with Cosine distance metric
	idx := NewHNSWIndex(Cosine, 8, 32, 16)

	// Insert vectors
	vectors := map[string][]float32{
		"A": {1.0, 0.0},
		"B": {0.0, 1.0},
		"C": {0.707, 0.707}, // 45 degrees, between A and B
		"D": {-1.0, 0.0},    // 180 degrees from A
	}

	for id, vec := range vectors {
		key := EncodeKey(id, vec)
		ref := storage.RecordRef{FileID: 1, Offset: int64(id[0]), Length: 10}
		if err := idx.Put(key, ref); err != nil {
			t.Fatalf("Put failed for node %s: %v", id, err)
		}
	}

	// 1. Search close to "A" (should return A, then C, then B)
	query := []float32{0.9, 0.1}
	refs, distances, err := idx.Search(query, 3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(refs) != 3 {
		t.Fatalf("expected 3 results, got %d", len(refs))
	}

	// Distance comparison: closest should be A, then C, then B
	expectedOrder := []int64{int64('A'), int64('C'), int64('B')}
	for i, ref := range refs {
		if ref.Offset != expectedOrder[i] {
			t.Errorf("results[%d]: expected Offset %d, got %d (distance=%f)", i, expectedOrder[i], ref.Offset, distances[i])
		}
	}

	// 2. Search close to "D" (should return D)
	queryD := []float32{-0.95, 0.05}
	refsD, _, err := idx.Search(queryD, 1)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(refsD) != 1 || refsD[0].Offset != int64('D') {
		t.Errorf("expected closest to be D, got %+v", refsD)
	}
}

func TestHNSWEuclideanSearch(t *testing.T) {
	// Initialize HNSW index with Euclidean distance metric
	idx := NewHNSWIndex(Euclidean, 4, 32, 16)

	// Put 2D spatial coordinates
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("point_%d", i)
		vec := []float32{float32(i), float32(i * 2)}
		key := EncodeKey(id, vec)
		ref := storage.RecordRef{FileID: 2, Offset: int64(i), Length: 8}
		idx.Put(key, ref)
	}

	// Search closest to coordinate (10.1, 20.2) -> should be point_10 (10.0, 20.0)
	query := []float32{10.1, 20.2}
	refs, _, err := idx.Search(query, 1)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(refs) != 1 || refs[0].Offset != 10 {
		t.Errorf("expected closest point to be index 10 (offset 10), got %+v", refs)
	}
}
