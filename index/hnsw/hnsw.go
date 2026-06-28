package hnsw

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/raman20/storage"
)

type DistanceMetric int

const (
	Euclidean DistanceMetric = iota
	Cosine
)

type HNSWIndex struct {
	mu           sync.RWMutex
	nodes        map[string]*HNSWNode
	enterPoint   *HNSWNode
	maxLayer     int
	metric       DistanceMetric
	m            int     // Max connections per node per layer
	m0           int     // Max connections for layer 0
	efConstruction int   // Size of dynamic candidate list during construction
	efSearch       int   // Size of dynamic candidate list during search
	levelMult    float64 // Normalization factor for level generation
	closed       bool
}

type HNSWNode struct {
	ID        string
	Vector    []float32
	Neighbors [][]string // Neighbors[level] is a list of node IDs at that level
	DataRef   storage.RecordRef
}

func NewHNSWIndex(metric DistanceMetric, m, efConstruction, efSearch int) *HNSWIndex {
	if m <= 0 {
		m = 16 // Default standard
	}
	if efConstruction <= 0 {
		efConstruction = 64
	}
	if efSearch <= 0 {
		efSearch = 32
	}

	return &HNSWIndex{
		nodes:          make(map[string]*HNSWNode),
		maxLayer:       -1,
		metric:         metric,
		m:              m,
		m0:             m * 2,
		efConstruction: efConstruction,
		efSearch:       efSearch,
		levelMult:      1.0 / math.Log(float64(m)),
	}
}

// distance calculates the distance between two vectors.
func (h *HNSWIndex) distance(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return math.MaxFloat32
	}

	switch h.metric {
	case Cosine:
		var dot, norm1, norm2 float32
		for i := 0; i < len(v1); i++ {
			dot += v1[i] * v2[i]
			norm1 += v1[i] * v1[i]
			norm2 += v2[i] * v2[i]
		}
		if norm1 == 0 || norm2 == 0 {
			return 1.0 // Maximum distance if zero-vector
		}
		return 1.0 - (dot / (float32(math.Sqrt(float64(norm1))) * float32(math.Sqrt(float64(norm2)))))

	case Euclidean:
		var sum float32
		for i := 0; i < len(v1); i++ {
			diff := v1[i] - v2[i]
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum)))
	default:
		return math.MaxFloat32
	}
}

func (h *HNSWIndex) generateRandomLevel() int {
	return int(-math.Log(rand.Float64()) * h.levelMult)
}

// DecodeKey extracts the ID and Vector from the composite index key.
// Format: [4B ID length] + [ID String] + [Float32 array]
func DecodeKey(key []byte) (string, []float32, error) {
	if len(key) < 4 {
		return "", nil, errors.New("invalid composite vector key: too short")
	}
	idLen := int(binary.BigEndian.Uint32(key[0:4]))
	if len(key) < 4+idLen {
		return "", nil, errors.New("invalid composite vector key: bad ID length")
	}
	id := string(key[4 : 4+idLen])

	vectorBytes := key[4+idLen:]
	if len(vectorBytes)%4 != 0 {
		return "", nil, errors.New("invalid composite vector key: float array size error")
	}

	dims := len(vectorBytes) / 4
	vector := make([]float32, dims)
	for i := 0; i < dims; i++ {
		bits := binary.BigEndian.Uint32(vectorBytes[i*4 : (i+1)*4])
		vector[i] = math.Float32frombits(bits)
	}

	return id, vector, nil
}

// EncodeKey constructs the composite key.
func EncodeKey(id string, vector []float32) []byte {
	idBytes := []byte(id)
	key := make([]byte, 4+len(idBytes)+len(vector)*4)
	binary.BigEndian.PutUint32(key[0:4], uint32(len(idBytes)))
	copy(key[4:4+len(idBytes)], idBytes)

	offset := 4 + len(idBytes)
	for _, val := range vector {
		binary.BigEndian.PutUint32(key[offset:offset+4], math.Float32bits(val))
		offset += 4
	}
	return key
}

func (h *HNSWIndex) Put(key []byte, ref storage.RecordRef) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return storage.ErrDBClosed
	}

	rawKey := key
	if decoded, err := hex.DecodeString(string(key)); err == nil {
		rawKey = decoded
	}

	id, vector, err := DecodeKey(rawKey)
	if err != nil {
		return err
	}

	// 1. Check if node already exists
	if _, exists := h.nodes[id]; exists {
		return fmt.Errorf("vector node %s already exists", id)
	}

	insertLevel := h.generateRandomLevel()
	newNode := &HNSWNode{
		ID:        id,
		Vector:    vector,
		Neighbors: make([][]string, insertLevel+1),
		DataRef:   ref,
	}
	for l := 0; l <= insertLevel; l++ {
		newNode.Neighbors[l] = []string{}
	}

	h.nodes[id] = newNode

	currEP := h.enterPoint
	if currEP == nil {
		h.enterPoint = newNode
		h.maxLayer = insertLevel
		return nil
	}

	// Step A: Search down from maxLayer to insertLevel
	dist := h.distance(vector, currEP.Vector)
	for l := h.maxLayer; l > insertLevel; l-- {
		changed := true
		for changed {
			changed = false
			for _, nID := range currEP.Neighbors[l] {
				neighbor := h.nodes[nID]
				d := h.distance(vector, neighbor.Vector)
				if d < dist {
					dist = d
					currEP = neighbor
					changed = true
				}
			}
		}
	}

	// Step B: Search and connect at each level from insertLevel down to 0
	eps := []*HNSWNode{currEP}
	for l := min(insertLevel, h.maxLayer); l >= 0; l-- {
		eps = h.searchLayer(vector, eps, h.efConstruction, l)
		
		// Connect neighbors to newNode at level l
		for _, ep := range eps {
			newNode.Neighbors[l] = append(newNode.Neighbors[l], ep.ID)
			ep.Neighbors[l] = append(ep.Neighbors[l], newNode.ID)

			// Prune connections if they exceed limit M
			maxConn := h.m
			if l == 0 {
				maxConn = h.m0
			}
			if len(ep.Neighbors[l]) > maxConn {
				h.pruneNeighbors(ep, l, maxConn)
			}
		}
	}

	if insertLevel > h.maxLayer {
		h.maxLayer = insertLevel
		h.enterPoint = newNode
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *HNSWIndex) searchLayer(query []float32, enterPoints []*HNSWNode, ef int, level int) []*HNSWNode {
	visited := make(map[string]bool)
	for _, ep := range enterPoints {
		visited[ep.ID] = true
	}

	candidates := make([]*HNSWNode, len(enterPoints))
	copy(candidates, enterPoints)

	results := make([]*HNSWNode, len(enterPoints))
	copy(results, enterPoints)

	// Sort helper: sort candidates by distance (closest first)
	sortByDistance := func(arr []*HNSWNode) {
		for i := 1; i < len(arr); i++ {
			key := arr[i]
			j := i - 1
			for j >= 0 && h.distance(query, arr[j].Vector) > h.distance(query, key.Vector) {
				arr[j+1] = arr[j]
				j = j - 1
			}
			arr[j+1] = key
		}
	}

	sortByDistance(candidates)
	sortByDistance(results)

	for len(candidates) > 0 {
		// Pop closest candidate
		curr := candidates[0]
		candidates = candidates[1:]

		furthestResultDist := h.distance(query, results[len(results)-1].Vector)
		if h.distance(query, curr.Vector) > furthestResultDist {
			break
		}

		for _, nID := range curr.Neighbors[level] {
			neighbor := h.nodes[nID]
			if !visited[nID] {
				visited[nID] = true
				d := h.distance(query, neighbor.Vector)
				furthestResultDist = h.distance(query, results[len(results)-1].Vector)

				if d < furthestResultDist || len(results) < ef {
					candidates = append(candidates, neighbor)
					results = append(results, neighbor)
					sortByDistance(candidates)
					sortByDistance(results)

					if len(results) > ef {
						results = results[:ef]
					}
				}
			}
		}
	}

	return results
}

func (h *HNSWIndex) pruneNeighbors(node *HNSWNode, level int, maxConn int) {
	neighbors := make([]*HNSWNode, len(node.Neighbors[level]))
	for i, nID := range node.Neighbors[level] {
		neighbors[i] = h.nodes[nID]
	}

	// Simple heuristic: keep the closest neighbors
	for i := 1; i < len(neighbors); i++ {
		key := neighbors[i]
		j := i - 1
		for j >= 0 && h.distance(node.Vector, neighbors[j].Vector) > h.distance(node.Vector, key.Vector) {
			neighbors[j+1] = neighbors[j]
			j = j - 1
		}
		neighbors[j+1] = key
	}

	if len(neighbors) > maxConn {
		neighbors = neighbors[:maxConn]
	}

	node.Neighbors[level] = make([]string, len(neighbors))
	for i, n := range neighbors {
		node.Neighbors[level][i] = n.ID
	}
}

// Search queries the top-K closest vectors in the index.
func (h *HNSWIndex) Search(query []float32, k int) ([]storage.RecordRef, []float32, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return nil, nil, storage.ErrDBClosed
	}

	currEP := h.enterPoint
	if currEP == nil || k <= 0 {
		return nil, nil, nil
	}

	dist := h.distance(query, currEP.Vector)
	// Navigate down layers
	for l := h.maxLayer; l > 0; l-- {
		changed := true
		for changed {
			changed = false
			for _, nID := range currEP.Neighbors[l] {
				neighbor := h.nodes[nID]
				d := h.distance(query, neighbor.Vector)
				if d < dist {
					dist = d
					currEP = neighbor
					changed = true
				}
			}
		}
	}

	// Final search in Layer 0
	eps := []*HNSWNode{currEP}
	results := h.searchLayer(query, eps, h.efSearch, 0)

	// Trim results to K
	if len(results) > k {
		results = results[:k]
	}

	refs := make([]storage.RecordRef, len(results))
	distances := make([]float32, len(results))
	for i, res := range results {
		refs[i] = res.DataRef
		distances[i] = h.distance(query, res.Vector)
	}

	return refs, distances, nil
}

func (h *HNSWIndex) Get(key []byte) (storage.RecordRef, bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	rawKey := key
	if decoded, err := hex.DecodeString(string(key)); err == nil {
		rawKey = decoded
	}

	id, _, err := DecodeKey(rawKey)
	if err != nil {
		node, found := h.nodes[string(key)]
		if found {
			return node.DataRef, true, nil
		}
		return storage.RecordRef{}, false, err
	}

	node, found := h.nodes[id]
	if !found {
		return storage.RecordRef{}, false, nil
	}

	return node.DataRef, true, nil
}

func (h *HNSWIndex) Delete(key []byte) error {
	return errors.New("HNSW direct index node deletion not supported (rebuild HNSW index for deletion)")
}

func (h *HNSWIndex) Scan(prefix []byte) ([]storage.RecordRef, error) {
	return nil, errors.New("vector scan not supported, use search")
}

func (h *HNSWIndex) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return nil
}

func (h *HNSWIndex) Stats() storage.IndexStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return storage.IndexStats{
		MemtableSize:   int64(len(h.nodes)), // Use count as size indicator
		ImmutableCount: 0,
		SSTableCount:   0,
		SSTableFiles:   nil,
	}
}
