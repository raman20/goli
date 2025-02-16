package wal

import (
	"bufio"
	"os"
	"strings"
)

type WAL struct {
	File *os.File
}

func NewWal(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		File: file,
	}, nil
}

func (wl *WAL) entry(key, value string) error {
	entry := key + ":" + value + "\n"
	_, err := wl.File.WriteString(entry)
	if err != nil {
		return err
	}
	return wl.File.Sync()
}

func (wl *WAL) read() (map[string]string, error) {
	entries := make(map[string]string)

	// Create a buffered reader for efficient streaming
	scanner := bufio.NewScanner(wl.File)

	// Read file line by line
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) == 2 {
			entries[parts[0]] = parts[1]
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}
