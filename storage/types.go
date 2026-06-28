package storage

// RecordRef represents the physical location of a record in the segment files.
type RecordRef struct {
	FileID uint32 // Segment file identifier
	Offset int64  // Starting byte offset in the segment file
	Length uint32 // Length of the payload in bytes
}

// RecordHeader contains metadata for a single log event.
type RecordHeader struct {
	Timestamp int64  // Unix nano timestamp of the record
	TxID      uint64 // Transaction identifier
	Type      byte   // Type of record (e.g., Write, Tombstone, System)
	KeyLen    uint32 // Length of the key in bytes
	ValLen    uint32 // Length of the value in bytes
}

// Index is the interface that all pluggable indexing layers must implement.
type Index interface {
	// Put inserts a key-value mapping to a RecordRef.
	Put(key []byte, ref RecordRef) error

	// Get retrieves the RecordRef mapped to the key.
	Get(key []byte) (RecordRef, bool, error)

	// Delete removes a key from the index.
	Delete(key []byte) error

	// Scan returns all RecordRefs whose keys match the given prefix.
	Scan(prefix []byte) ([]RecordRef, error)
}
