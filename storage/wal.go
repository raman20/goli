package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type WAL struct {
	File   *os.File
	writer *bufio.Writer
}

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

func (wl *WAL) Close() error {
	if err := wl.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	if err := wl.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	return wl.File.Close()
}

func (wl *WAL) Entry(key, value string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], keyLen)
	binary.BigEndian.PutUint32(header[4:8], valLen)

	if _, err := wl.writer.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	if _, err := wl.writer.WriteString(key); err != nil {
		return fmt.Errorf("failed to write WAL key: %w", err)
	}
	if _, err := wl.writer.WriteString(value); err != nil {
		return fmt.Errorf("failed to write WAL value: %w", err)
	}

	if err := wl.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	return wl.File.Sync()
}

func (wl *WAL) Read() (map[string]string, error) {
	if _, err := wl.File.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek WAL file: %w", err)
	}

	entries := make(map[string]string)
	reader := bufio.NewReader(wl.File)

	var header [8]byte
	for {
		_, err := io.ReadFull(reader, header[:])
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			// io.ErrUnexpectedEOF could occur if a write was partially written before a crash;
			// in a real WAL we would truncate the file, but here we can just stop recovering.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL entry header: %w", err)
		}

		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return nil, fmt.Errorf("failed to read WAL key: %w", err)
		}

		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuf); err != nil {
			return nil, fmt.Errorf("failed to read WAL value: %w", err)
		}

		entries[string(keyBuf)] = string(valBuf)
	}

	return entries, nil
}

