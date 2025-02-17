package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
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
	if strings.Contains(key, ":") {
		return errors.New("key cannot contain ':' character")
	}

	entry := key + ":" + value + "\n"
	_, err := wl.writer.WriteString(entry)
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
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
	scanner := bufio.NewScanner(wl.File)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid WAL entry format at line %d", lineNum)
		}
		entries[parts[0]] = parts[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL file: %w", err)
	}

	return entries, nil
}
