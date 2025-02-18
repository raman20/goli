# 🚀 Goli: A Log-Structured Merge Tree Database in Go

Goli is a high-performance key-value store implemented in Go, based on the Log-Structured Merge (LSM) tree architecture. It's designed for high write throughput while maintaining good read performance.

## 🌟 Features

### Currently Implemented
- **In-Memory Storage**
  - Skip List-based Memtable for fast reads/writes
  - Size-based rotation of memtables
  - Concurrent operations support

- **Persistence**
  - Write-Ahead Log (WAL) for durability
  - Crash recovery from WAL
  - Atomic operations

- **Architecture**
  - LSM tree foundation
  - Two-level memory structure (active + immutable memtables)
  - Efficient memory management

- **Concurrency**
  - Thread-safe operations
  - Lock-free reads where possible
  - Background compaction support

### 🎯 Upcoming Features

#### Short Term
- [ ] SSTable Implementation
  - Sorted string table format
  - Block-based storage
  - Index and bloom filters
  - Compression support

- [ ] Compaction
  - Level-based compaction strategy
  - Background compaction workers
  - Size-tiered compaction

- [ ] Performance Optimizations
  - Bloom filters for negative lookups
  - Block cache for hot data
  - Configurable compression

#### Long Term
- [ ] Advanced Features
  - Range queries
  - Snapshots
  - Transactions
  - Custom comparators
  - Prefix iteration

- [ ] Operational Features
  - Metrics and monitoring
  - Backup/restore
  - Online compaction
  - Data migration tools

## 🏗️ Architecture

```
┌─────────────────┐
│ Client API      │
└────────┬────────┘
         │
┌────────▼────────┐   ┌─────────────┐
│ Active Memtable │   │ Write-Ahead │
│ (Skip List)     ├──►│     Log     │
└────────┬────────┘   └─────────────┘
         │
         │ (when full)
         ▼
┌─────────────────┐
│ Immutable       │
│ Memtable        │
└────────┬────────┘
         │
         │ (background flush)
         ▼
┌─────────────────┐
│ SSTable         │
│ (Coming)        │
└─────────────────┘
```

## 🎯 Design Goals

1. **High Write Throughput**: Optimized for write-heavy workloads
2. **Predictable Latency**: Minimal GC impact and consistent performance
3. **Durability**: No data loss on crashes
4. **Scalability**: Efficient handling of large datasets
5. **Simplicity**: Clean, maintainable codebase

## 🔍 Implementation Details

### Memory Management
- Two-phase memory structure
- Size-based rotation
- Controlled memory footprint

### Persistence
- Write-Ahead Logging
- Crash recovery
- Future SSTable support

### Concurrency
- Fine-grained locking
- Lock-free reads
- Background compaction

## 🤝 Contributing

Contributions are welcome! Areas that need attention:
1. SSTable implementation
2. Compaction strategies
3. Performance optimizations
4. Testing and benchmarking
5. Documentation

## 📊 Performance

(Coming soon: Benchmark results comparing different workloads and configurations)

## 📝 License

MIT License - feel free to use in your own projects!

## 🙏 Acknowledgments

Inspired by:
- LevelDB
- RocksDB