package goli

import (
	"fmt"

	"go.etcd.io/bbolt"
)

type Database struct {
	db   *bbolt.DB
	name string
}

func New(name string) (*Database, error) {
	db, err := bbolt.Open(name+".db", 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &Database{db: db, name: name}, nil
}

func (db *Database) GetCollection(name string) (*Collection, error) {
	// ** Begin a transaction to ensure atomicity of operations **
	tx, err := db.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // ** Ensure rollback on error to maintain data integrity **

	bucket := tx.Bucket([]byte(name))
	if bucket != nil {
		// If the bucket already exists, return the existing collection
		return &Collection{name: name, bucket: bucket, db: db}, nil
	}

	// ** Create a new bucket within the transaction **
	bucket, err = tx.CreateBucket([]byte(name))
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	// ** Commit the transaction to save changes **
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &Collection{name: name, bucket: bucket, db: db}, nil
}

func (db *Database) Close() error {
	return db.db.Close()
}
