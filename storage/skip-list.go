package storage

import (
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
)

type SkipList struct {
	head       *SkipListNode
	p          float64
	currLevel  int
	maxLevel   int
	mu         sync.RWMutex
	size       int
	comparator func(a, b string) int // Allow custom comparators
}

type SkipListNode struct {
	key    string
	value  string
	levels []*SkipListNode
}

func InitSL(p float64, maxLevel int) *SkipList {
	if p <= 0 || p >= 1 {
		p = 0.5 // Default probability
	}
	if maxLevel <= 0 {
		maxLevel = 16 // Default max level
	}

	return &SkipList{
		head: &SkipListNode{
			key:    "",
			value:  "",
			levels: make([]*SkipListNode, maxLevel),
		},
		p:          p,
		maxLevel:   maxLevel,
		currLevel:  1,
		comparator: strings.Compare,
	}
}

func (sl *SkipList) Put(key string, value string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	levelTrack := make([]*SkipListNode, sl.maxLevel)

	if sl.head == nil {
		sl.head = &SkipListNode{
			key:    key,
			value:  value,
			levels: make([]*SkipListNode, sl.maxLevel),
		}
		sl.currLevel = 1
		sl.size++
		return nil
	}

	currentNode := sl.head

	for i := sl.currLevel - 1; i >= 0; i-- {
		for currentNode.levels[i] != nil && currentNode.levels[i].key < key {
			currentNode = currentNode.levels[i]
		}

		levelTrack[i] = currentNode
	}

	newLevel := sl.levelUp()
	if newLevel > sl.currLevel {
		for i := sl.currLevel; i < newLevel; i++ {
			levelTrack[i] = sl.head
		}
		sl.currLevel = newLevel
	}

	newNode := &SkipListNode{
		key:    key,
		value:  value,
		levels: make([]*SkipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.levels[i] = levelTrack[i].levels[i]
		levelTrack[i].levels[i] = newNode
	}

	// Check for duplicate key
	if currentNode.key == key {
		currentNode.value = value // Update value if key exists
		sl.size++
		return nil
	}

	sl.size++
	return nil
}

func (sl *SkipList) Get(key string) (string, bool) {
	current := sl.head

	// Start from the highest level and move down
	for i := sl.currLevel - 1; i >= 0; i-- {
		for current.levels[i] != nil && current.levels[i].key < key {
			current = current.levels[i] // Move forward
		}
	}

	// Move to level 0 and check the next node
	current = current.levels[0]
	if current != nil && current.key == key {
		return current.value, true // Key found
	}

	return "", false // Key not found
}

func (sl *SkipList) Delete(key string) bool {
	update := make([]*SkipListNode, sl.maxLevel)
	current := sl.head

	// Step 1: Locate node and track updates
	for i := sl.currLevel - 1; i >= 0; i-- {
		for current.levels[i] != nil && current.levels[i].key < key {
			current = current.levels[i]
		}
		update[i] = current // Store the last node before key
	}

	// Step 2: Move to the node to be deleted
	current = current.levels[0]
	if current == nil || current.key != key {
		return false // Key not found
	}

	// Step 3: Update pointers at each level
	for i := 0; i < sl.currLevel; i++ {
		if update[i].levels[i] != current {
			break
		}
		update[i].levels[i] = current.levels[i]
	}

	// Step 4: Reduce currentLevel if necessary
	for sl.currLevel > 1 && sl.head.levels[sl.currLevel-1] == nil {
		sl.currLevel--
	}

	sl.size--
	return true
}

func (sl *SkipList) levelUp() int {
	level := 1
	for rand.Float64() < sl.p && level < sl.maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

type Iterator struct {
	current *SkipListNode
	sl      *SkipList
}

func (sl *SkipList) Iterator() *Iterator {
	return &Iterator{
		current: sl.head,
		sl:      sl,
	}
}

func (it *Iterator) Next() bool {
	if it.current == nil || it.current.levels[0] == nil {
		return false
	}
	it.current = it.current.levels[0]
	return true
}

func (it *Iterator) Key() string {
	if it.current == nil {
		return ""
	}
	return it.current.key
}

func (it *Iterator) Value() string {
	if it.current == nil {
		return ""
	}
	return it.current.value
}
