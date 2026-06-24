package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/raman20/server"
	"github.com/raman20/storage"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	dataDir := flag.String("data-dir", "data", "directory for database files")
	dbName := flag.String("db-name", "goli_store", "name of the database instance")
	flag.Parse()

	log.Printf("Starting Goli LSM Object Store...")

	// 1. Initialize Options
	opts := storage.DefaultOptions()
	opts.DataDir = *dataDir
	// Set memtable size to 2MB for demonstration
	opts.MemtableSize = 2 * 1024 * 1024

	// 2. Open Goli DB
	db, err := storage.Open(*dbName, opts)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		log.Printf("Closing database...")
		db.Close()
	}()

	// 3. Initialize Server
	objectDir := *dataDir + "/objects"
	srv, err := server.NewServer(db, objectDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// 4. Start HTTP Server
	addr := fmt.Sprintf(":%d", *port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: srv,
	}

	go func() {
		log.Printf("Server listening on http://localhost%s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Handle graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Printf("Shutting down gracefully...")
}
