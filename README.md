# 🚀 Goli: A Log-Structured Merge Tree Database in Go

Goli is a high-performance, thread-safe, and durable key-value storage engine implemented in Go. It is built from scratch using the **Log-Structured Merge (LSM) Tree** architecture, optimized for write-heavy workloads while maintaining fast read lookups and range scans.

---

## 🌟 Key Architectural Components

Goli splits your database operations into high-speed memory writes, transactional logging, and sequential segment storage:

### 🪵 1. Segment Storage Core
- **Segment Manager**: The low-level database manager ([segment.go](file:///home/raman/goli/storage/segment.go)) responsible for writing raw byte data sequentially to segment files and returning unique 16-byte coordinates (`RecordRef`).
- **Pointers-Only Indexes**: Search indexes (like LSM, Vector, or Streaming) never store values directly. They map search keys to a lightweight `RecordRef` (FileID, Offset, Length).
- **Concurrency**: Leverages thread-safe concurrent file reading (`ReadAt`) to allow multiple readers to query segments simultaneously without locks.

### 🧠 2. Memtable & SkipList
- **Active Memtable**: All writes (`Set`/`Delete`) are buffered in a thread-safe, memory-resident **SkipList** ([skip-list.go](file:///home/raman/goli/storage/skip-list.go)).
- **Concurrent-Safe**: Uses fine-grained reader/writer locking ([db.go](file:///home/raman/goli/storage/db.go)) to guarantee safe concurrent reads and writes.
- **Rotation**: When the active memtable size exceeds `MemtableSize`, it is rotated to an **Immutable Memtable**, and a new active memtable is seamlessly spawned to accept incoming traffic without blocking.

### 📝 3. Transactional Write-Ahead Log (WAL)
- **ACID Durability**: Every write operation is written to the Write-Ahead Log ([wal.go](file:///home/raman/goli/storage/wal.go)) in transaction batches (`[TX_START]...[Writes]...[TX_COMMIT]`) before updating the memtable, guaranteeing zero data loss on crash.
- **Crash Recovery**: Replays WAL logs sequentially on startup, replaying only successfully committed transactions and discarding incomplete/aborted writes.

### 💾 4. SSTables (Sorted String Tables)
- **Disk Persistence**: Background workers flush immutable memtables to disk as Sorted String Tables ([sstable.go](file:///home/raman/goli/storage/sstable.go)).
- **Efficient Indexing**: Each SSTable contains:
  - **Data Block**: Sequential sorted key-value pairs.
  - **Index Block**: Placed at the end of the file, allowing fast in-memory binary searching of keys to identify file offsets.
  - **Footer**: A fixed-size block containing the offset of the index block and a validation magic number (`0x53535442`).

### ⚙️ 5. Asynchronous K-Way Compaction
- **Space Reclamation**: Flushed SSTables are compacted asynchronously in the background once the count exceeds `CompactionThreshold`.
- **K-Way Merge**: Merges overlapping SSTables, keeping only the newest version of duplicate keys and purging tombstones (deleted items) to recover valuable disk space.
- **Lock-Free IO**: The compaction process runs independently without holding the database lock, ensuring that client reads and writes remain highly responsive.

### 🔍 6. Prefix Range Queries
- **Cross-Layer Scan**: `DB.Scan(prefix)` traverses the active memtable, immutable memtables, and all loaded SSTable index blocks, merging keys from newest to oldest and filtering out tombstones.

---

## 🏗️ System Architecture

```
                       ┌─────────────────────────┐
                       │       Client API        │
                       │   (Get / Set / Delete)  │
                       └────────────┬────────────┘
                                    │
                                    ▼
                      ┌──────────────────────────┐
                      │     Goli LSM Engine      │
                      └──────┬────────────┬──────┘
                             │            │
             (Append Log)    │            │ (Write Buffer)
                             ▼            ▼
                     ┌───────────┐   ┌───────────────────────────┐
                     │ Transac-  │   │ Active Memtable           │
                     │ tional WAL│   │ (In-Memory SkipList)      │
                     └───────────┘   └────────────┬──────────────┘
                                                  │
                                                  │ (Rotate if full)
                                                  ▼
                                     ┌───────────────────────────┐
                                     │ Immutable Memtable        │
                                     │ (Awaiting Flush)          │
                                     └────────────┬──────────────┘
                                                  │
                                                  │ (Background Flush)
                                                  ▼
                                     ┌───────────────────────────┐
                                     │ Sorted String Table (sst) │
                                     │   ┌───────────────────┐   │
                                     │   │    Data Block     │   │
                                     │   ├───────────────────┤   │
                                     │   │    Index Block    │   │
                                     │   ├───────────────────┤   │
                                     │   │    SSTB Footer    │   │
                                     │   └───────────────────┘   │
                                     └────────────┬──────────────┘
                                                  │
                                                  │ (Compaction Merge)
                                                  ▼
                                     ┌───────────────────────────┐
                                     │  Compacted Single SSTable │
                                     └───────────────────────────┘
```

---

## 📁 Directory Structure

```text
├── storage/
│   ├── db.go             # Main database engine lifecycle and compaction coordinator
│   ├── db_test.go        # End-to-end integration and concurrency tests
│   ├── skip-list.go      # Thread-safe SkipList data structure
│   ├── skip_list_test.go # SkipList unit tests (insert, duplicate updates, delete)
│   ├── sstable.go        # Sorted String Table reader, writer, and iterator
│   ├── sstable_test.go   # SSTable read/write and binary lookup validation tests
│   ├── wal.go            # Transactional Write-Ahead Log implementation
│   ├── wal_test.go       # WAL transaction and crash-recovery tests
│   ├── segment.go        # Sequential segment storage manager
│   ├── segment_test.go   # Segment Manager concurrency and rollover tests
│   └── types.go          # Core data models (RecordRef, Index interface)
├── bin/                  # Compiled executable binaries (ignored by git)
├── Makefile              # Project lifecycle script definitions (build, test, run)
├── main.go               # Interactive LSM KV CLI prompt/REPL script
└── README.md             # Project documentation (you are here)
```

---

## 🚀 Getting Started

### 📋 Prerequisites
Ensure you have **Go 1.23+** installed on your machine.

### 🔨 Installation & Tests
Clone the repository and run the test suite to verify the integrity of the storage engine:
```bash
# Run all tests (SkipList, WAL, SSTables, Segments, DB, Compaction)
make test
```

### 💻 Run the Interactive Database Shell (CLI)
Compile and run the interactive CLI:
```bash
# Compile and start the interactive database shell
make run
```
This opens the Goli interactive prompt (`goli> `). You can execute the following database operations:
* `set <key> <value>`: Store a key-value pair.
* `get <key>`: Retrieve the value of a key.
* `delete <key>`: Delete a key (writes a tombstone).
* `scan <prefix>`: Scan and list all keys matching the prefix.
* `stats`: Show engine metrics (active memtable size, counts of files).
* `exit` / `quit`: Safely exit the database shell.

You can also run one-off CLI commands directly:
```bash
# Example: write a key
./bin/goli set app_version 1.0.0

# Example: read stats
./bin/goli stats
```

---

## 💡 Quick Code Example

Here is how to integrate Goli into your Go applications:

```go
package main

import (
	"fmt"
	"log"
	
	"github.com/raman20/storage"
)

func main() {
	// 1. Initialize Default Database Options
	opts := storage.DefaultOptions()
	opts.DataDir = "my_data_directory"
	opts.MemtableSize = 4 * 1024 * 1024 // 4MB
	opts.CompactionThreshold = 4

	// 2. Open Goli DB Instance
	db, err := storage.Open("user_store", opts)
	if err != nil {
		log.Fatalf("Failed to open Goli: %v", err)
	}
	defer db.Close() // Safely flushes buffers & closes active logs

	// 3. Write Keys
	if err := db.Set("user:100:profile", `{"name":"Raman","age":30}`); err != nil {
		log.Fatalf("Set failed: %v", err)
	}

	// 4. Read Keys
	if val, ok := db.Get("user:100:profile"); ok {
		fmt.Printf("User Profile: %s\n", val)
	} else {
		fmt.Println("User Profile not found!")
	}

	// 5. Scan Range by Prefix
	results, err := db.Scan("user:")
	if err == nil {
		for key, val := range results {
			fmt.Printf("Found: %s -> %s\n", key, val)
		}
	}

	// 6. Delete Key (Writes a Tombstone)
	db.Delete("user:100:profile")
}
```

---

## 📝 License
Goli is distributed under the **MIT License**. Feel free to use it in your personal or commercial applications!