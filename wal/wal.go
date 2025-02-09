package wal

import (
	"fmt"
	"os"
)

type OperationType byte

const (
	PUT    OperationType = 1
	DELETE OperationType = 2
)

type WALEntry struct {
	SequenceNumber uint64
	Operation      OperationType
	KeySize        uint32
	Key            string
	ValueSize      uint32
	Value          string
}

type WAL struct {
	File   *os.File
	SeqNum uint64
}

func OpenWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		File:   file,
		SeqNum: 0,
	}, nil
}

func (wal *WAL) WALWrite(entry WALEntry) error {
	wal.SeqNum++

	line := fmt.Sprintf("%d %d %d %s %d %s\n",
		wal.SeqNum,
		entry.Operation,
		entry.KeySize, entry.Key,
		entry.ValueSize, entry.Value,
	)

	_, err := wal.File.WriteString(line)
	if err != nil {
		return err
	}

	return wal.File.Sync()
}

func (wal *WAL) WALRead() ([]WALEntry, error) {
	file, err := os.Open(wal.File.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var enteries []WALEntry

	for {
		var seq uint64
		var op OperationType
		var keySize, valueSize uint32
		var key, value string

		_, err := fmt.Fscan(file, "%d %d %d %s %d %s\n",
			&seq, &op, &keySize, &key, &valueSize, &value,
		)

		if err != nil {
			break
		}

		enteries = append(enteries, WALEntry{
			SequenceNumber: seq,
			Operation:      op,
			KeySize:        keySize,
			Key:            key,
			ValueSize:      valueSize,
			Value:          value,
		})
	}
	return enteries, nil
}
func (wal *WAL) Close() error {
	return wal.File.Close()
}
