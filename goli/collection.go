package goli

import (
	"fmt"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

type Collection struct {
	name   string
	bucket *bbolt.Bucket
	db     *Database
}

func (c *Collection) Create(data map[string]string) (uuid.UUID, error) {
	id := uuid.New()

	// ** Begin a transaction to ensure atomicity of operations **
	tx, err := c.db.db.Begin(true)
	if err != nil {
		return id, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // ** Ensure rollback on error to maintain data integrity **

	if err := tx.Bucket([]byte(c.name)).Put([]byte("id"), []byte(id.String())); err != nil {
		return id, fmt.Errorf("failed to store ID: %w", err)
	}

	for k, v := range data {
		if err := tx.Bucket([]byte(c.name)).Put([]byte(k), []byte(v)); err != nil {
			return id, fmt.Errorf("failed to put data: %w", err)
		}
	}

	// ** Commit the transaction to save changes **
	if err := tx.Commit(); err != nil {
		return id, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return id, nil
}

func (c *Collection) Read(query map[string]string) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range query {
		value := c.bucket.Get([]byte(k))
		if value == nil || string(value) != v {
			return nil, fmt.Errorf("query (%s: %s) not found in collection (%s)", k, v, c.name)
		}
		result[k] = string(value)
	}
	return result, nil
}

func (c *Collection) Update() {}
func (c *Collection) Delete() {}
