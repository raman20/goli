package main

import (
	"fmt"
	"log"

	"github.com/raman20/wal"
)

func main() {
	wl, err := wal.OpenWAL("test.log")
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer wl.Close()

	entries := []wal.WALEntry{
		{Operation: wal.PUT, Key: "foo", Value: "bar", KeySize: 3, ValueSize: 3},
		{Operation: wal.PUT, Key: "hello", Value: "world", KeySize: 5, ValueSize: 5},
		{Operation: wal.DELETE, Key: "foo", Value: "", KeySize: 3, ValueSize: 0},
	}

	for _, entry := range entries {
		if err := wl.WALWrite(entry); err != nil {
			log.Fatalf("Failed to write entry: %v", err)
		}
	}

	fmt.Println("✅ WAL entries written successfully!")

	readEntries, err := wl.WALRead()
	if err != nil {
		log.Fatalf("Failed to read WAL: %v", err)
	}

	// Print recovered WAL entries
	fmt.Println("\n🔄 Recovered WAL Entries:")
	for _, e := range readEntries {
		fmt.Printf("Seq: %d | Op: %d | Key: %s | Value: %s\n", e.SequenceNumber, e.Operation, e.Key, e.Value)
	}

}
