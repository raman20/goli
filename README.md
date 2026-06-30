# 🚀 Goli: A Unified Storage Engine with Pluggable Index Lenses

Goli is a modular, first-principles database storage engine written in Go. Instead of coupling storage formats to database models (like traditional key-value or vector databases), Goli decouples physical byte storage from querying logic entirely. 

A single, sequential **Unified Storage Core** handles write-ahead logging and raw byte persistence, while **Pluggable Indexing Layers (Lenses)** organize search indexes for Key-Value, Vector similarity search, and beyond.

---

## 🌟 Core Architectural Pillars

### 🪵 1. Unified Storage Core (Segment Files)
* **Sequential Value Logging**: The low-level database manager ([segment.go](file:///home/raman/goli/storage/segment.go)) appends raw payload bytes sequentially to disk segments and returns a 16-byte coordinate pointer (**`RecordRef`**).
* **Zero-Lock Concurrency**: Reads execute using thread-safe random reads (`ReadAt`) directly from segment files, allowing concurrent queries without lock contention.
* **Immutability**: Once a segment reaches its limit (e.g. 64MB), it is closed and becomes read-only, making it ready for cache mapping or cloud-tiering.

### 🔌 2. Pluggable Indexing Lenses
Search indexes never store value payloads directly. Instead, they act as read-only "Lenses" that map query targets to physical coordinate pointers:
* **KV Index (LSM)**: String Key $\rightarrow$ `RecordRef` (LSM Tree / SkipList).
* **Vector Index (HNSW)**: Float Array $\rightarrow$ `RecordRef` (HNSW Graph).

### ⚡ 3. Native Key-Value Separation (WiscKey)
By separating the raw value payloads in the segment files (acting as the WiscKey Value Log / Vlog) from the sorted keys in the index ([lsm_index.go](file:///home/raman/goli/index/lsm/lsm_index.go)), Goli eliminates write amplification. Compaction only runs on tiny key-pointer pairs, bypassing all heavy data payloads entirely.

### ⚖️ 4. Zero-Configuration Dynamic Indexing (Lazy-Loading)
Every collection in Goli is just a sequential directory. Indexes are **lazy-loaded plugins** loaded on-demand:
* Goli loads the primary LSM (KV) index by default.
* Goli dynamically initializes the HNSW vector index **only on the first vector write (`vset`)**. If a collection is only used for KV, HNSW consumes `0` RAM and file descriptors.

---

## 🏗️ System Architecture

```
                       ┌───────────────────────────┐
                       │     Client Operations     │
                       │   (KV, Vector, Streaming) │
                       └─────────────┬─────────────┘
                                     │
                                     ▼
                       ┌───────────────────────────┐
                       │    Goli Unified Engine    │
                       └─────────────┬─────────────┘
                                     │
                                     ▼ (Save Payload)
                       ┌───────────────────────────┐
                       │   Segment Storage Core    │
                       │   (Vlog / 00000001.seg)   │
                       └─────────────┬─────────────┘
                                     │ (Returns RecordRef)
                                     ▼
                       ┌───────────────────────────┐
                       │  Pluggable Index Lenses   │
                       │ ┌───────────────┬───────┐ │
                       │ │   LSM Index   │ HNSW  │ │
                       │ │ (Auto-Loaded) │ (Lazy)│ │
                       │ └───────────────┴───────┘ │
                       └───────────────────────────┘
```

---

## 📁 Directory Structure

The Goli codebase is strictly modularized into isolated library packages and executables:

```text
├── cmd/
│   └── goli/
│       └── main.go       # Interactive CLI shell prompt/REPL script
├── index/
│   ├── hnsw/
│   │   ├── hnsw.go       # Vector similarity search graph index lens
│   │   └── hnsw_test.go  # Cosine & Euclidean similarity search tests
│   └── lsm/
│       ├── lsm_index.go  # LSM Index interface coordinator
│       ├── memtable.go   # Memtable manager
│       ├── skip-list.go  # Thread-safe SkipList structure
│       ├── sstable.go    # Sorted String Table reader & writer
│       ├── sstable_test.go
│       └── skip_list_test.go
├── storage/
│   ├── db.go             # Main database engine orchestrator
│   ├── db_test.go        # End-to-end integration tests
│   ├── segment.go        # Sequential segment storage manager (Vlog)
│   ├── segment_test.go   # Segment concurrency and rollover tests
│   ├── types.go          # Core models (RecordRef, Index interface)
│   ├── wal.go            # Transactional Write-Ahead Log
│   └── wal_test.go       # WAL transaction tests
├── bin/                  # Compiled executable binaries
├── Makefile              # Project build definition script
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

This opens the Goli interactive prompt (`goli[default_kv]> `). Available commands:
* **Collection Management**:
  * `collection create <name>`: Create a collection namespace.
  * `collection list`: List all discovered collections.
  * `use <collection>`: Switch the active collection context.
* **Key-Value Operations**:
  * `set <key> <value>`: Store a key-value pair.
  * `get <key>`: Retrieve the value of a key (also retrieves vector metadata by ID!).
  * `delete <key>`: Delete a key (purges from all active indexes).
  * `scan <prefix>`: Scan and list keys matching the prefix.
* **Vector Operations**:
  * `vset <id> <vector_csv> <metadata_value>`: Insert vector coordinates & metadata (e.g. `vset A 0.1,0.2 {"name":"A"}`).
  * `vsearch <vector_csv> <k>`: Search top-k nearest neighbor vectors (e.g. `vsearch 0.1,0.19 1`).
  * `vstats`: Show HNSW graph vector stats.

---

## 💡 Quick Go Integration Code Example

```go
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/raman20/index/hnsw"
	"github.com/raman20/index/lsm"
	"github.com/raman20/storage"
)

func main() {
	opts := storage.DefaultOptions()
	opts.DataDir = "my_data"

	// 1. Setup directories for the LSM Index
	dbPath := filepath.Join(opts.DataDir, "user_store")
	walPath := filepath.Join(dbPath, "wal")
	sstPath := filepath.Join(dbPath, "sst")
	_ = os.MkdirAll(walPath, 0755)
	_ = os.MkdirAll(sstPath, 0755)

	// 2. Initialize the pluggable LSM Index (Primary Index)
	lsmIdx, err := lsm.NewLSMIndex(walPath, sstPath, opts)
	if err != nil {
		log.Fatalf("Failed to initialize LSM Index: %v", err)
	}

	// 3. Open Goli DB instance
	db, err := storage.Open("user_store", opts, lsmIdx)
	if err != nil {
		log.Fatalf("Failed to open Goli: %v", err)
	}
	defer db.Close()

	// 4. Lazy-load secondary HNSW index on demand
	hnswIdx := db.GetOrInitIndex("vector", func() storage.Index {
		return hnsw.NewHNSWIndex(hnsw.Cosine, 16, 64, 32)
	}).(*hnsw.HNSWIndex)

	// 5. Insert Vector (Updates both HNSW and LSM indexes)
	compositeKey := hnsw.EncodeKey("item_100", []float32{0.1, 0.2})
	if err := db.InsertVector(compositeKey, `{"name":"Premium Chair","price":99.00}`); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	// 6. Point Lookup (Fast O(log N) lookup directly by text ID via LSM index)
	if val, ok := db.Get("item_100"); ok {
		fmt.Printf("Get item_100: %s\n", val)
	}

	// 7. Vector Similarity Search (HNSW index traversal)
	refs, distances, err := hnswIdx.Search([]float32{0.1, 0.19}, 1)
	if err == nil && len(refs) > 0 {
		payload, _ := db.ReadRecord(refs[0])
		fmt.Printf("Vector Search: Match=%s, Distance=%f\n", payload, distances[0])
	}
}
```
