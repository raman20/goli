package goli

import (
	"fmt"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

type Goli struct {
	db *bbolt.DB
}

type Collection struct {
	bucket *bbolt.Bucket
}

func New() (*Goli, error) {
	db, err := bbolt.Open("goli.db", 0666, nil)
	if err != nil {
		return nil, err
	}

	return &Goli{
		db: db,
	}, nil
}

func (g *Goli) CreateCollection(name string) (*Collection, error) {
	tx, err := g.db.Begin(true)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return &Collection{bucket: bucket}, nil
}

func (g *Goli) Put(collName string, data map[string]string) (uuid.UUID, error) {
	id := uuid.New()
	tx, err := g.db.Begin(true)
	if err != nil {
		return id, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return id, err
	}

	for k, v := range data {
		if err := bucket.Put([]byte(k), []byte(v)); err != nil {
			return id, err
		}
	}

	if err := bucket.Put([]byte("id"), []byte(id.String())); err != nil {
		return id, err
	}

	return id, tx.Commit()
}

func (g *Goli) Get(collName string, query map[string]string) (map[string]string, error) {
	tx, err := g.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket([]byte(collName))
	if bucket == nil {
		return nil, fmt.Errorf("Collection (%s) not found", collName)
	}

}
