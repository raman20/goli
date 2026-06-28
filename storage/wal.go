package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	TxStart  byte = 1
	TxCommit byte = 2
	TxSet    byte = 3
	TxDelete byte = 4
)

type WAL struct {
	File   *os.File
	writer *bufio.Writer
}

type RecoveredOp struct {
	Key    string
	Value  string
	Delete bool
}

// InitWal initializes and opens a WAL file at the given path.
func InitWal(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		File:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

// Close flushes the writer and closes the file descriptor.
func (wl *WAL) Close() error {
	if err := wl.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	if err := wl.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	return wl.File.Close()
}

// WriteTxStart writes the transaction start marker.
func (wl *WAL) WriteTxStart(txID uint64) error {
	var buf [9]byte
	buf[0] = TxStart
	binary.BigEndian.PutUint64(buf[1:9], txID)
	_, err := wl.writer.Write(buf[:])
	return err
}

// WriteTxCommit writes the transaction commit marker and syncs changes to disk.
func (wl *WAL) WriteTxCommit(txID uint64) error {
	var buf [9]byte
	buf[0] = TxCommit
	binary.BigEndian.PutUint64(buf[1:9], txID)
	if _, err := wl.writer.Write(buf[:]); err != nil {
		return err
	}
	if err := wl.writer.Flush(); err != nil {
		return err
	}
	return wl.File.Sync()
}

// WriteTxSet writes a key-value write operation inside a transaction.
func (wl *WAL) WriteTxSet(txID uint64, key, value string) error {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	var header [17]byte
	header[0] = TxSet
	binary.BigEndian.PutUint64(header[1:9], txID)
	binary.BigEndian.PutUint32(header[9:13], keyLen)
	binary.BigEndian.PutUint32(header[13:17], valLen)

	if _, err := wl.writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := wl.writer.WriteString(key); err != nil {
		return err
	}
	if _, err := wl.writer.WriteString(value); err != nil {
		return err
	}
	return nil
}

// WriteTxDelete writes a key deletion operation inside a transaction.
func (wl *WAL) WriteTxDelete(txID uint64, key string) error {
	keyLen := uint32(len(key))

	var header [13]byte
	header[0] = TxDelete
	binary.BigEndian.PutUint64(header[1:9], txID)
	binary.BigEndian.PutUint32(header[9:13], keyLen)

	if _, err := wl.writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := wl.writer.WriteString(key); err != nil {
		return err
	}
	return nil
}

// Read reads the WAL sequentially and returns all recovered committed operations.
// Transactions that do not have a TX_COMMIT are discarded.
func (wl *WAL) Read() ([]RecoveredOp, error) {
	if _, err := wl.File.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek WAL file: %w", err)
	}

	var committedOps []RecoveredOp
	txBuffers := make(map[uint64][]RecoveredOp)
	reader := bufio.NewReader(wl.File)

	for {
		entryTypeByte, err := reader.ReadByte()
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL entry type: %w", err)
		}

		var txIDBuf [8]byte
		if _, err := io.ReadFull(reader, txIDBuf[:]); err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("failed to read WAL txID: %w", err)
		}
		txID := binary.BigEndian.Uint64(txIDBuf[:])

		switch entryTypeByte {
		case TxStart:
			txBuffers[txID] = []RecoveredOp{}

		case TxCommit:
			if ops, exists := txBuffers[txID]; exists {
				committedOps = append(committedOps, ops...)
				delete(txBuffers, txID)
			}

		case TxSet:
			var lenBuf [8]byte
			if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("failed to read WAL key/val length: %w", err)
			}
			keyLen := binary.BigEndian.Uint32(lenBuf[0:4])
			valLen := binary.BigEndian.Uint32(lenBuf[4:8])

			keyBuf := make([]byte, keyLen)
			if _, err := io.ReadFull(reader, keyBuf); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("failed to read WAL key: %w", err)
			}

			valBuf := make([]byte, valLen)
			if _, err := io.ReadFull(reader, valBuf); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("failed to read WAL value: %w", err)
			}

			op := RecoveredOp{
				Key:    string(keyBuf),
				Value:  string(valBuf),
				Delete: false,
			}
			txBuffers[txID] = append(txBuffers[txID], op)

		case TxDelete:
			var lenBuf [4]byte
			if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("failed to read WAL key length: %w", err)
			}
			keyLen := binary.BigEndian.Uint32(lenBuf[:])

			keyBuf := make([]byte, keyLen)
			if _, err := io.ReadFull(reader, keyBuf); err != nil {
				if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("failed to read WAL key: %w", err)
			}

			op := RecoveredOp{
				Key:    string(keyBuf),
				Delete: true,
			}
			txBuffers[txID] = append(txBuffers[txID], op)
		}
	}

	return committedOps, nil
}
