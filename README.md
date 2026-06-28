# 🚀 Goli: A Unified Storage Engine with Pluggable Indexing

Goli is a modular, first-principles database storage engine written in Go. Instead of coupling storage formats to database models, Goli decouples physical byte storage from querying logic. A single, sequential **Unified Storage Core** handles write-ahead logging and raw data persistence, while **Pluggable Indexing Layers (Lenses)** organize search indexes for Key-Value, Vector similarity search, and Streaming/Messaging workloads.

---

## 🌟 Core Architectural Pillars

### 🪵 1. Unified Storage Core (Segment Files)
- **Sequential Append**: The low-level database manager ([segment.go](file:///home/raman/goli/storage/segment.go)) appends raw payload bytes sequentially to disk segments and returns a 16-byte coordinate pointer (**`RecordRef`**).
- **Zero-Lock Concurrency**: Reads are executed using thread-safe random reads (`ReadAt`) directly from segment files, allowing concurrent queries without lock contention.
- **Immutability**: Once a segment reaches its limit (e.g. 64MB), it is closed and becomes read-only, making it ready for cloud-tiering.

### 🔌 2. Pluggable Indexing Lenses
Search indexes never store value payloads directly. Instead, they act as read-only "Lenses" that map query targets (keys, vectors, text tokens) to physical coordinate pointers:
- **KV Index**: String Key $\rightarrow$ `RecordRef` (LSM Tree / SkipList).
- **Vector Index**: Float Array $\rightarrow$ `RecordRef` (HNSW Graph).
- **Streaming Index**: Sequential Offset $\rightarrow$ `RecordRef` (Commit Log).
- **Full-Text Index**: Token Word $\rightarrow$ List of `RecordRef`s (Inverted Index).

### ⚡ 3. Native Key-Value Separation (WiscKey)
By separating the raw value payloads in the segment files (acting as the WiscKey Value Log / Vlog) from the sorted keys in the index ([lsm_index.go](file:///home/raman/goli/storage/lsm_index.go)), Goli eliminates write amplification. Compaction only runs on tiny key-pointer pairs, bypassing all heavy data payloads entirely.

### ☁️ 4. Polyglot Cloud Storage Tiering (Compute & Storage Separation)
Goli supports distinct storage systems for indexes and data segments. For example:
- **`IndexStorage`** can be routed to a low-latency cache or local SSD for microsecond query lookups.
- **`SegmentStorage`** can be routed directly to cost-efficient cloud object stores (like AWS S3 or MinIO) using HTTP Range gets to fetch raw values on-demand.

---

## 🏗️ System Architecture

```
                       ┌─────────────────────────┐
                       │    Client Operations    │
                       │  (KV, Vector, Streaming)│
                       └────────────┬────────────┘
                                    │
                                    ▼
                      ┌──────────────────────────┐
                      │   Goli Unified Engine    │
                      └──────┬────────────┬──────┘
                             │            │
             (Append Log)    │            │ (Save Payload)
                             ▼            ▼
                     ┌───────────┐   ┌───────────────────────────┐
                     │ Transac-  │   │  Segment Storage Core     │
                     │ tional WAL│   │   (segment.go / Vlog)     │
                     └───────────┘   └────────────┬──────────────┘
                                                  │ (Returns RecordRef)
                                                  ▼
                                     ┌───────────────────────────┐
                                     │ Pluggable Index Lenses    │
                                     │ ┌───────────────┬───────┐ │
                                     │ │   LSM Index   │ HNSW  │ │
                                     │ │  (KV / Cache) │(Vector│ │
                                     │ └───────────────┴───────┘ │
                                     └───────────────────────────┘
```

---

## 📁 Directory Structure

```text
├── storage/
│   ├── db.go             # Main database engine orchestrator
│   ├── db_test.go        # End-to-end database integration tests
│   ├── segment.go        # Sequential segment storage manager (Vlog)
│   ├── segment_test.go   # Segment Manager concurrency and rollover tests
│   ├── types.go          # Core data models (RecordRef, Index interface)
│   ├── lsm_index.go      # LSM Index implementation (key-coordinate indexing)
│   ├── lsm_index_test.go # LSM Index point write, read, and delete tests
│   ├── skip-list.go      # Thread-safe SkipList data structure
│   ├── skip_list_test.go # SkipList unit tests
│   ├── sstable.go        # Sorted String Table reader, writer, and index blocks
│   ├── sstable_test.go   # SSTable read/write and binary lookup tests
│   ├── wal.go            # Transactional Write-Ahead Log
│   └── wal_test.go       # WAL transaction and crash-recovery tests
├── bin/                  # Compiled executable binaries (ignored by git)
├── Makefile              # Project lifecycle script definitions (build, test, run)
├── main.go               # Interactive CLI shell prompt/REPL script
└── README.md             # Project documentation (you are here)
```

---

## 🚀 Getting Started

### 🔨 Run all Tests
Validate the integrity of the storage, indexing, WAL, and compaction layers:
```bash
make test
```

### 💻 Run the Interactive Database Shell
Compile and run the interactive CLI shell:
```bash
make run
```
This opens the Goli interactive prompt (`goli> `). You can execute:
* `set <key> <value>`: Store a key-value pair.
* `get <key>`: Retrieve the value of a key.
* `delete <key>`: Delete a key (writes a tombstone).
* `scan <prefix>`: Scan and list all keys matching the prefix.
* `stats`: Show engine metrics.
* `exit` / `quit`: Safely exit the database shell.

---

## 💡 Quick Go Integration Code Example

```go
package main

import (
	"fmt"
	"log"
	
	"github.com/raman20/storage"
)

func main() {
	opts := storage.DefaultOptions()
	opts.DataDir = "my_data"

	// Open Goli DB instance
	db, err := storage.Open("user_store", opts)
	if err != nil {
		log.Fatalf("Failed to open Goli: %v", err)
	}
	defer db.Close()

	// Write key (Payload stored in segment file, RecordRef mapped in LSMIndex)
	if err := db.Set("user:100:profile", `{"name":"Raman"}`); err != nil {
		log.Fatalf("Set failed: %v", err)
	}

	// Read key (Queries coordinates from LSMIndex, reads value from segment file)
	if val, ok := db.Get("user:100:profile"); ok {
		fmt.Printf("User Profile: %s\n", val)
	}
}
```