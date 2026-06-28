package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/raman20/index/lsm"
	"github.com/raman20/storage"
)

func main() {
	opts := storage.DefaultOptions()
	opts.DataDir = "data"
	// Set memtable size to 1MB for normal CLI use
	opts.MemtableSize = 1024 * 1024

	dbPath := filepath.Join(opts.DataDir, "goli_db")
	walPath := filepath.Join(dbPath, "wal")
	sstPath := filepath.Join(dbPath, "sst")
	_ = os.MkdirAll(walPath, 0755)
	_ = os.MkdirAll(sstPath, 0755)

	lsmIdx, err := lsm.NewLSMIndex(walPath, sstPath, opts)
	if err != nil {
		log.Fatalf("Failed to initialize LSM Index: %v", err)
	}

	db, err := storage.Open("goli_db", opts, lsmIdx)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// If arguments are provided, run as single-shot CLI command
	if len(os.Args) > 1 {
		runSingleCommand(db, os.Args[1:])
		return
	}

	// Otherwise, start interactive REPL shell
	runREPL(db)
}

func runSingleCommand(db *storage.DB, args []string) {
	cmd := strings.ToLower(args[0])
	switch cmd {
	case "set", "put":
		if len(args) < 3 {
			fmt.Println("Usage: goli set <key> <value>")
			os.Exit(1)
		}
		key := args[1]
		val := strings.Join(args[2:], " ")
		if err := db.Set(key, val); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: goli get <key>")
			os.Exit(1)
		}
		val, ok := db.Get(args[1])
		if !ok {
			fmt.Println("(nil)")
		} else {
			fmt.Println(val)
		}

	case "del", "delete":
		if len(args) < 2 {
			fmt.Println("Usage: goli delete <key>")
			os.Exit(1)
		}
		if err := db.Delete(args[1]); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "scan":
		if len(args) < 2 {
			fmt.Println("Usage: goli scan <prefix>")
			os.Exit(1)
		}
		results, err := db.Scan(args[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		if len(results) == 0 {
			fmt.Println("(empty)")
		} else {
			for k, v := range results {
				fmt.Printf("%s => %s\n", k, v)
			}
		}

	case "stats":
		stats := db.Stats()
		fmt.Printf("Active Memtable Size:     %d bytes\n", stats.MemtableSize)
		fmt.Printf("Immutable Memtable Count: %d\n", stats.ImmutableCount)
		fmt.Printf("SSTable File Count:       %d\n", stats.SSTableCount)
		if len(stats.SSTableFiles) > 0 {
			fmt.Printf("SSTable Files:\n")
			for _, file := range stats.SSTableFiles {
				fmt.Printf("  - %s\n", file)
			}
		}

	default:
		fmt.Printf("Unknown command: %s. Supported: set, get, delete, scan, stats\n", cmd)
		os.Exit(1)
	}
}

func runREPL(db *storage.DB) {
	fmt.Println("🚀 Welcome to the Goli LSM Database Interactive Shell")
	fmt.Println("Type \"help\" for list of commands, \"exit\" or \"quit\" to quit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("goli> ")
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
			fmt.Println("  set <key> <value>   - Store a key-value pair")
			fmt.Println("  get <key>           - Retrieve value for a key")
			fmt.Println("  delete <key>        - Delete a key")
			fmt.Println("  scan <prefix>       - List keys matching a prefix")
			fmt.Println("  stats               - Show engine metrics")
			fmt.Println("  exit / quit         - Exit the shell")
			continue
		}

		runSingleCommand(db, parts)
	}
}
