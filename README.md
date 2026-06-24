# 🚀 Goli: A Log-Structured Merge Tree Database in Go

Goli is a high-performance key-value database implemented in Go, based on the Log-Structured Merge (LSM) tree architecture. It is optimized for high-speed writes while keeping disk and memory footprint low.

---

## 🌟 Key Features

- **Skip List-based Memtable**: Thread-safe in-memory buffer with lock-free reads.
- **Robust Binary WAL**: Length-prefixed Write-Ahead Log for crash recovery. Safe for binary payloads, colons, and newlines.
- **SSTable Storage**: Disk persistence with block-based indexes and footer verification.
- **Background Compaction**: K-way merge compaction that merges SSTables, discards tombstones (deletions), and reclaims disk space asynchronously.
- **Prefix Range Queries**: Quick scanning of key spaces via `DB.Scan(prefix)`.

---

## 🏗️ Architecture

```
                                 ┌───────────────────────────────┐
                                 │          Client API           │
                                 └───────────────┬───────────────┘
                                                 │
                                 ┌───────────────▼───────────────┐
                                 │        Goli LSM Engine        │
                                 │ ┌───────────────────────────┐ │
                                 │ │ Active Memtable (SkipList)│ │
                                 │ └─────────────┬─────────────┘ │
                                 │               │ (Flush)       │
                                 │               ▼               │
                                 │ ┌───────────────────────────┐ │
                                 │ │ Sorted String Tables (sst)│ │
                                 │ └───────────────────────────┘ │
                                 └───────────────────────────────┘
```

---

## 🚀 Getting Started

### 📋 Prerequisites
- Go 1.23 or higher

### 🔨 Compilation & Testing
Run all tests to verify database, WAL, SSTable, and core engine functionality:
```bash
make test
```

Build the binary:
```bash
make build
```

Run the interactive engine demo:
```bash
make run
```

---

## 💻 Code Example

```go
package main

import (
	"fmt"
	"log"
	"github.com/raman20/storage"
)

func main() {
	// Configure options
	opts := storage.DefaultOptions()
	opts.DataDir = "data"

	// Open the database
	db, err := storage.Open("my_db", opts)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Write keys
	db.Set("user:name", "Raman")

	// Read keys
	if val, ok := db.Get("user:name"); ok {
		fmt.Printf("Found: %s\n", val)
	}

	// Delete keys
	db.Delete("user:name")
}
```