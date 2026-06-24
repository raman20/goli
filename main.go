package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/raman20/storage"
)

func main() {
	log.Println("=== Starting Goli LSM Key-Value Engine Demo ===")

	dataDir := "data_demo"
	// Clean up any old demo files to start fresh
	os.RemoveAll(dataDir)

	// 1. Configure Options
	opts := storage.DefaultOptions()
	opts.DataDir = dataDir
	// Set memtable size to 256 bytes so that inserting even a few keys triggers flushes and SSTable creation!
	opts.MemtableSize = 256
	opts.CompactionThreshold = 3

	// 2. Open Goli DB
	db, err := storage.Open("demo_db", opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	fmt.Println("\n--- [1] Writing initial keys (Writing to active Memtable & WAL) ---")
	db.Set("user:1:name", "Raman")
	db.Set("user:1:role", "Admin")
	db.Set("user:2:name", "Alice")
	
	fmt.Printf("Get(user:1:name) -> ")
	if val, ok := db.Get("user:1:name"); ok {
		fmt.Printf("Found: %q\n", val)
	} else {
		fmt.Printf("Not Found\n")
	}

	fmt.Println("\n--- [2] Writing more keys to trigger Memtable rotation & SSTable Flushes ---")
	// These writes will exceed the 256 bytes memtable limit and force rotations to disk
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("extra:key:%d", i)
		val := fmt.Sprintf("value_payload_data_string_%d", i)
		db.Set(key, val)
	}

	// Give a tiny window for background goroutines (flushes & compactions) to finish disk I/O
	time.Sleep(150 * time.Millisecond)

	fmt.Println("\n--- [3] Performing prefix scanning across Memtable + SSTables ---")
	results, err := db.Scan("extra:")
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}
	fmt.Printf("Found %d keys starting with 'extra:':\n", len(results))
	for k, v := range results {
		fmt.Printf("  %s => %s\n", k, v)
	}

	fmt.Println("\n--- [4] Testing Deletion (Tombstone) ---")
	db.Delete("user:2:name")
	if _, ok := db.Get("user:2:name"); !ok {
		fmt.Println("user:2:name is successfully deleted (Get returned Not Found)")
	}

	fmt.Println("\n--- [5] Closing the database and preparing for crash recovery demo ---")
	db.Close()

	fmt.Println("\n--- [6] Re-opening the database (Recovering state from SSTables & WAL) ---")
	db2, err := storage.Open("demo_db", opts)
	if err != nil {
		log.Fatalf("Failed to re-open database: %v", err)
	}
	defer db2.Close()

	fmt.Println("\n--- [7] Querying recovered state ---")
	if val, ok := db2.Get("user:1:name"); ok {
		fmt.Printf("Recovered user:1:name: %q (expected \"Raman\")\n", val)
	} else {
		fmt.Printf("Failed to recover user:1:name!\n")
	}

	if _, ok := db2.Get("user:2:name"); !ok {
		fmt.Println("Recovered user:2:name tombstone: Still deleted (expected Not Found)")
	}

	// Verify we can read the flushed keys from SSTables
	if val, ok := db2.Get("extra:key:5"); ok {
		fmt.Printf("Recovered extra:key:5: %q (expected \"value_payload_data_string_5\")\n", val)
	} else {
		fmt.Printf("Failed to recover extra:key:5!\n")
	}

	fmt.Println("\n=== Goli LSM Demo completed successfully! ===")
}
