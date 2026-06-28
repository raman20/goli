package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/raman20/index/hnsw"
	"github.com/raman20/index/lsm"
	"github.com/raman20/storage"
)

type MultiModelDB struct {
	opts        storage.Options
	activeName  string
	collections map[string]bool
	openedDBs   map[string]*storage.DB
	openedLSMs  map[string]*lsm.LSMIndex
	openedHNSWs map[string]*hnsw.HNSWIndex
}

func main() {
	opts := storage.DefaultOptions()
	opts.DataDir = "data"
	opts.MemtableSize = 1024 * 1024 // 1MB

	mmDB := &MultiModelDB{
		opts:        opts,
		collections: make(map[string]bool),
		openedDBs:   make(map[string]*storage.DB),
		openedLSMs:  make(map[string]*lsm.LSMIndex),
		openedHNSWs: make(map[string]*hnsw.HNSWIndex),
	}

	// Dynamic Collection Auto-Discovery
	if err := mmDB.discoverCollections(); err != nil {
		log.Fatalf("Failed to discover collections: %v", err)
	}

	// Close all connection pools on exit
	defer mmDB.Close()

	if len(os.Args) > 1 {
		runSingleCommand(mmDB, os.Args[1:])
		return
	}

	runREPL(mmDB)
}

func (m *MultiModelDB) discoverCollections() error {
	colsDir := filepath.Join(m.opts.DataDir, "collections")
	_ = os.MkdirAll(colsDir, 0755)

	entries, err := os.ReadDir(colsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			m.collections[entry.Name()] = true
		}
	}

	// If no collections found, default to default_kv
	if len(m.collections) == 0 {
		m.collections["default_kv"] = true
	}

	// Set default active collection to the first discovered one
	for name := range m.collections {
		m.activeName = name
		break
	}

	return nil
}

func (m *MultiModelDB) GetActiveDB() (*storage.DB, *lsm.LSMIndex, *hnsw.HNSWIndex, error) {
	if db, exists := m.openedDBs[m.activeName]; exists {
		return db, m.openedLSMs[m.activeName], m.openedHNSWs[m.activeName], nil
	}

	// Initialize collection storage paths
	colPath := filepath.Join(m.opts.DataDir, "collections", m.activeName)
	walPath := filepath.Join(colPath, "wal")
	sstPath := filepath.Join(colPath, "sst")
	_ = os.MkdirAll(walPath, 0755)
	_ = os.MkdirAll(sstPath, 0755)

	dbOpts := m.opts
	dbOpts.DataDir = colPath

	// All Goli collections use LSMIndex as their primary index for key/ID mapping
	lsmIdx, err := lsm.NewLSMIndex(walPath, sstPath, dbOpts)
	if err != nil {
		return nil, nil, nil, err
	}

	db, err := storage.Open(m.activeName, dbOpts, lsmIdx)
	if err != nil {
		lsmIdx.Close()
		return nil, nil, nil, err
	}

	m.openedDBs[m.activeName] = db
	m.openedLSMs[m.activeName] = lsmIdx
	m.openedHNSWs[m.activeName] = nil // Lazy-loaded on demand

	return db, lsmIdx, nil, nil
}

func (m *MultiModelDB) Close() {
	for _, db := range m.openedDBs {
		db.Close()
	}
}

func parseVector(vecStr string) ([]float32, error) {
	parts := strings.Split(vecStr, ",")
	vec := make([]float32, len(parts))
	for i, p := range parts {
		var val float32
		_, err := fmt.Sscanf(strings.TrimSpace(p), "%f", &val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float %q: %w", p, err)
		}
		vec[i] = val
	}
	return vec, nil
}

func runSingleCommand(db *MultiModelDB, args []string) {
	cmd := strings.ToLower(args[0])

	switch cmd {
	case "collection":
		if len(args) < 2 {
			fmt.Println("Usage: goli collection create <name> | collection list")
			return
		}
		subCmd := strings.ToLower(args[1])

		if subCmd == "create" {
			if len(args) < 3 {
				fmt.Println("Usage: goli collection create <name>")
				return
			}
			name := args[2]
			colPath := filepath.Join(db.opts.DataDir, "collections", name)
			_ = os.MkdirAll(colPath, 0755)
			db.collections[name] = true
			fmt.Println("OK")
			return

		} else if subCmd == "list" {
			for name := range db.collections {
				activeMarker := " "
				if name == db.activeName {
					activeMarker = "*"
				}
				fmt.Printf("%s %s\n", activeMarker, name)
			}
			return
		}

	case "use":
		if len(args) < 2 {
			fmt.Println("Usage: goli use <collection_name>")
			return
		}
		name := args[1]
		if !db.collections[name] {
			fmt.Printf("Error: collection %q does not exist\n", name)
			return
		}
		db.activeName = name
		fmt.Printf("Switched to collection %q\n", name)
		return
	}

	kvDB, _, _, err := db.GetActiveDB()
	if err != nil {
		fmt.Printf("Error opening collection %s: %v\n", db.activeName, err)
		return
	}

	switch cmd {
	case "set", "put":
		if len(args) < 3 {
			fmt.Println("Usage: goli set <key> <value>")
			return
		}
		key := args[1]
		val := strings.Join(args[2:], " ")
		if err := kvDB.Set(key, val); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK")

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: goli get <key>")
			return
		}
		val, ok := kvDB.Get(args[1])
		if !ok {
			fmt.Println("(nil)")
		} else {
			fmt.Println(val)
		}

	case "del", "delete":
		if len(args) < 2 {
			fmt.Println("Usage: goli delete <key>")
			return
		}
		if err := kvDB.Delete(args[1]); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println("OK")

	case "scan":
		if len(args) < 2 {
			fmt.Println("Usage: goli scan <prefix>")
			return
		}
		results, err := kvDB.Scan(args[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		if len(results) == 0 {
			fmt.Println("(empty)")
		} else {
			for k, v := range results {
				fmt.Printf("%s => %s\n", k, v)
			}
		}

	case "stats":
		stats := kvDB.Stats()
		fmt.Printf("Active Memtable Size:     %d bytes\n", stats.MemtableSize)
		fmt.Printf("Immutable Memtable Count: %d\n", stats.ImmutableCount)
		fmt.Printf("SSTable File Count:       %d\n", stats.SSTableCount)

	case "vset":
		if len(args) < 4 {
			fmt.Println("Usage: goli vset <id> <vector_csv> <metadata_value>")
			return
		}
		id := args[1]
		vec, err := parseVector(args[2])
		if err != nil {
			fmt.Printf("Error parsing vector: %v\n", err)
			return
		}
		metadata := strings.Join(args[3:], " ")

		// Lazy initialize the HNSW vector index on the first write
		_ = kvDB.GetOrInitIndex("vector", func() storage.Index {
			hnswIdx := hnsw.NewHNSWIndex(hnsw.Cosine, 16, 64, 32)
			db.openedHNSWs[db.activeName] = hnswIdx
			return hnswIdx
		})

		compositeKey := hnsw.EncodeKey(id, vec)
		if err := kvDB.InsertVector(compositeKey, metadata); err != nil {
			fmt.Printf("Error writing payload: %v\n", err)
			return
		}
		fmt.Println("OK")

	case "vsearch":
		if len(args) < 3 {
			fmt.Println("Usage: goli vsearch <vector_csv> <k>")
			return
		}
		vec, err := parseVector(args[1])
		if err != nil {
			fmt.Printf("Error parsing vector: %v\n", err)
			return
		}
		k, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Printf("Invalid k: %v\n", err)
			return
		}

		idx, exists := kvDB.GetIndex("vector")
		if !exists {
			fmt.Println("(empty - no vectors indexed yet)")
			return
		}
		hnswIdx := idx.(*hnsw.HNSWIndex)

		refs, distances, err := hnswIdx.Search(vec, k)
		if err != nil {
			fmt.Printf("Vector search error: %v\n", err)
			return
		}

		if len(refs) == 0 {
			fmt.Println("(empty)")
			return
		}

		for i, ref := range refs {
			payload, err := kvDB.ReadRecord(ref)
			if err != nil {
				payload = fmt.Sprintf("(failed to read payload: %v)", err)
			}
			fmt.Printf("Result %d: Distance=%f | Metadata=%s\n", i+1, distances[i], payload)
		}

	case "vstats":
		idx, exists := kvDB.GetIndex("vector")
		if !exists {
			fmt.Println("HNSW Indexed Vectors: 0")
			return
		}
		hnswIdx := idx.(*hnsw.HNSWIndex)
		stats := hnswIdx.Stats()
		fmt.Printf("HNSW Indexed Vectors: %d\n", stats.MemtableSize)

	default:
		fmt.Printf("Unknown command: %s. Supported: set, get, delete, scan, stats, vset, vsearch, vstats, collection, use\n", cmd)
	}
}

func runREPL(db *MultiModelDB) {
	fmt.Println("🚀 Welcome to the Goli Multi-Model Database Shell")
	fmt.Println("Type \"help\" for list of commands, \"exit\" or \"quit\" to quit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("goli[%s]> ", db.activeName)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		if cmd == "exit" || cmd == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		if cmd == "help" {
			fmt.Println("Available commands:")
			fmt.Println("  collection create <name>           - Create a collection")
			fmt.Println("  collection list                    - List all collections")
			fmt.Println("  use <collection_name>              - Switch the active collection")
			fmt.Println("  set <key> <value>                  - Store a KV entry")
			fmt.Println("  get <key>                          - Retrieve a KV entry (works on vector metadata too!)")
			fmt.Println("  delete <key>                       - Delete a KV entry (removes from all indexes!)")
			fmt.Println("  scan <prefix>                      - Scan KV by prefix")
			fmt.Println("  stats                              - Show active collection engine metrics")
			fmt.Println("  vset <id> <vector> <val>           - Insert vector node (auto-activates HNSW graph)")
			fmt.Println("  vsearch <vector> <k>               - Search nearest vectors (only if vectors indexed)")
			fmt.Println("  vstats                             - Show HNSW vector index metrics")
			fmt.Println("  exit / quit                        - Exit the shell")
			continue
		}

		runSingleCommand(db, parts)
	}
}
