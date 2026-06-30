package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

const MagicNumber uint32 = 0x53535442 // "SSTB" in hex

type IndexEntry struct {
	Key    string
	Offset int64
	ValLen uint32
}

type SSTable struct {
	filePath string
	file     *os.File
	index    []IndexEntry
}

// WriteSSTable writes a SkipList/Memtable to an SSTable file.
func WriteSSTable(filePath string, iterator *Iterator) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer file.Close()

	var index []IndexEntry
	var offset int64

	// Write data block
	for iterator.Next() {
		key := iterator.Key()
		val := iterator.Value()

		keyLen := uint32(len(key))
		valLen := uint32(len(val))

		// Write key length, value length
		var header [8]byte
		binary.BigEndian.PutUint32(header[0:4], keyLen)
		binary.BigEndian.PutUint32(header[4:8], valLen)

		if _, err := file.Write(header[:]); err != nil {
			return fmt.Errorf("failed to write data header: %w", err)
		}

		// Save offset of the key-value pair in file
		index = append(index, IndexEntry{
			Key:    key,
			Offset: offset,
			ValLen: valLen,
		})

		// Write key
		if _, err := file.WriteString(key); err != nil {
			return fmt.Errorf("failed to write data key: %w", err)
		}
		// Write value
		if _, err := file.WriteString(val); err != nil {
			return fmt.Errorf("failed to write data value: %w", err)
		}

		offset += 8 + int64(keyLen) + int64(valLen)
	}

	// Write index block
	indexOffset := offset
	for _, entry := range index {
		keyLen := uint32(len(entry.Key))
		var idxHeader [16]byte
		binary.BigEndian.PutUint32(idxHeader[0:4], keyLen)
		binary.BigEndian.PutUint64(idxHeader[4:12], uint64(entry.Offset))
		binary.BigEndian.PutUint32(idxHeader[12:16], entry.ValLen)

		if _, err := file.Write(idxHeader[:]); err != nil {
			return fmt.Errorf("failed to write index header: %w", err)
		}
		if _, err := file.WriteString(entry.Key); err != nil {
			return fmt.Errorf("failed to write index key: %w", err)
		}
		offset += 16 + int64(keyLen)
	}

	// Write Footer
	var footer [16]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint32(footer[8:12], uint32(len(index)))
	binary.BigEndian.PutUint32(footer[12:16], MagicNumber)

	if _, err := file.Write(footer[:]); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return file.Sync()
}

// OpenSSTable opens an SSTable file and loads its index block into memory.
func OpenSSTable(filePath string) (*SSTable, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat SSTable file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize < 16 {
		file.Close()
		return nil, fmt.Errorf("invalid SSTable file size: too small")
	}

	// Read footer (last 16 bytes)
	var footer [16]byte
	if _, err := file.ReadAt(footer[:], fileSize-16); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	numKeys := binary.BigEndian.Uint32(footer[8:12])
	magic := binary.BigEndian.Uint32(footer[12:16])

	if magic != MagicNumber {
		file.Close()
		return nil, fmt.Errorf("invalid SSTable magic number: %x", magic)
	}

	// Read index block
	index := make([]IndexEntry, numKeys)
	if _, err := file.Seek(indexOffset, 0); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to index block: %w", err)
	}

	for i := uint32(0); i < numKeys; i++ {
		var idxHeader [16]byte
		if _, err := io.ReadFull(file, idxHeader[:]); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index entry header: %w", err)
		}

		keyLen := binary.BigEndian.Uint32(idxHeader[0:4])
		offset := int64(binary.BigEndian.Uint64(idxHeader[4:12]))
		valLen := binary.BigEndian.Uint32(idxHeader[12:16])

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index key: %w", err)
		}

		index[i] = IndexEntry{
			Key:    string(keyBuf),
			Offset: offset,
			ValLen: valLen,
		}
	}

	return &SSTable{
		filePath: filePath,
		file:     file,
		index:    index,
	}, nil
}

// Get checks if the key exists in this SSTable and returns the value if found.
func (sst *SSTable) Get(key string) (string, bool, error) {
	// Binary search on index
	idx := sort.Search(len(sst.index), func(i int) bool {
		return sst.index[i].Key >= key
	})

	if idx < len(sst.index) && sst.index[idx].Key == key {
		entry := sst.index[idx]
		
		// Read value from file
		valOffset := entry.Offset + 8 + int64(len(key))
		valBuf := make([]byte, entry.ValLen)
		if _, err := sst.file.ReadAt(valBuf, valOffset); err != nil {
			return "", false, fmt.Errorf("failed to read value from SSTable: %w", err)
		}
		
		return string(valBuf), true, nil
	}

	return "", false, nil
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}

func (sst *SSTable) FilePath() string {
	return sst.filePath
}

func (sst *SSTable) FileHandle() *os.File {
	return sst.file
}

func (sst *SSTable) IndexBlock() []IndexEntry {
	return sst.index
}

type SSTableIterator struct {
	sst     *SSTable
	currIdx int
	currKey string
	currVal string
	err     error
}

func (sst *SSTable) Iterator() *SSTableIterator {
	return &SSTableIterator{
		sst:     sst,
		currIdx: -1,
	}
}

func (it *SSTableIterator) Next() bool {
	it.currIdx++
	if it.currIdx >= len(it.sst.index) {
		return false
	}
	
	entry := it.sst.index[it.currIdx]
	valOffset := entry.Offset + 8 + int64(len(entry.Key))
	valBuf := make([]byte, entry.ValLen)
	_, err := it.sst.file.ReadAt(valBuf, valOffset)
	if err != nil {
		it.err = fmt.Errorf("failed to read value at index %d: %w", it.currIdx, err)
		return false
	}
	
	it.currKey = entry.Key
	it.currVal = string(valBuf)
	return true
}

func (it *SSTableIterator) Key() string {
	return it.currKey
}

func (it *SSTableIterator) Value() string {
	return it.currVal
}

func (it *SSTableIterator) Error() error {
	return it.err
}

func (it *SSTableIterator) CurrIdx() int {
	return it.currIdx
}

func (it *SSTableIterator) SSTable() *SSTable {
	return it.sst
}
