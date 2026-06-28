package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type SegmentManager struct {
	dir          string
	maxSize      int64
	activeId     uint32
	activeFile   *os.File
	activeOffset int64
	files        map[uint32]*os.File
	mu           sync.RWMutex
}

// NewSegmentManager initializes and returns a SegmentManager.
func NewSegmentManager(dir string, maxSize int64) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	if maxSize <= 0 {
		maxSize = 64 * 1024 * 1024 // 64MB default
	}

	sm := &SegmentManager{
		dir:   dir,
		maxSize: maxSize,
		files: make(map[uint32]*os.File),
	}

	// Scan directory for existing segments
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment directory: %w", err)
	}

	var maxId uint32
	for _, entry := range entries {
		var id uint32
		if _, err := fmt.Sscanf(entry.Name(), "%08d.seg", &id); err == nil {
			if id > maxId {
				maxId = id
			}
		}
	}

	// Start with the latest existing segment, or segment ID 1
	if maxId == 0 {
		maxId = 1
	}
	sm.activeId = maxId

	activePath := filepath.Join(dir, fmt.Sprintf("%08d.seg", sm.activeId))
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open active segment: %w", err)
	}
	sm.activeFile = file
	sm.files[sm.activeId] = file

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat active segment: %w", err)
	}
	sm.activeOffset = stat.Size()

	return sm, nil
}

// Append writes raw payload bytes sequentially to the active segment file.
func (sm *SegmentManager) Append(data []byte) (RecordRef, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	dataLen := int64(len(data))
	// If writing this data exceeds max segment size, roll over
	if sm.activeOffset+dataLen > sm.maxSize {
		if err := sm.rollOver(); err != nil {
			return RecordRef{}, err
		}
	}

	ref := RecordRef{
		FileID: sm.activeId,
		Offset: sm.activeOffset,
		Length: uint32(dataLen),
	}

	n, err := sm.activeFile.Write(data)
	if err != nil {
		return RecordRef{}, fmt.Errorf("failed to write to segment %d: %w", sm.activeId, err)
	}
	sm.activeOffset += int64(n)

	return ref, nil
}

func (sm *SegmentManager) rollOver() error {
	// Sync active file before roll over
	if err := sm.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment %d: %w", sm.activeId, err)
	}

	sm.activeId++
	activePath := filepath.Join(sm.dir, fmt.Sprintf("%08d.seg", sm.activeId))
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new segment %d: %w", sm.activeId, err)
	}
	sm.activeFile = file
	sm.files[sm.activeId] = file
	sm.activeOffset = 0

	return nil
}

// Read reads and returns raw payload bytes from a specific segment coordinate.
func (sm *SegmentManager) Read(ref RecordRef) ([]byte, error) {
	sm.mu.RLock()
	file, exists := sm.files[ref.FileID]
	sm.mu.RUnlock()

	if !exists {
		sm.mu.Lock()
		// Double check under write lock
		file, exists = sm.files[ref.FileID]
		if !exists {
			filePath := filepath.Join(sm.dir, fmt.Sprintf("%08d.seg", ref.FileID))
			var err error
			file, err = os.OpenFile(filePath, os.O_RDONLY, 0644)
			if err != nil {
				sm.mu.Unlock()
				return nil, fmt.Errorf("failed to open segment file %d: %w", ref.FileID, err)
			}
			sm.files[ref.FileID] = file
		}
		sm.mu.Unlock()
	}

	buf := make([]byte, ref.Length)
	_, err := file.ReadAt(buf, ref.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read from segment %d at offset %d: %w", ref.FileID, ref.Offset, err)
	}

	return buf, nil
}

// Close closes all open segment files.
func (sm *SegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var firstErr error
	for id, file := range sm.files {
		if file == sm.activeFile {
			file.Sync()
		}
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close segment %d: %w", id, err)
		}
	}
	sm.files = make(map[uint32]*os.File)
	sm.activeFile = nil
	return firstErr
}
