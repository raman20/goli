package memtable

import (
	"github.com/raman20/utils"
	"github.com/raman20/wal"
)

type Memtable struct {
	wal  *wal.WAL
	data *utils.SkipList
}

func Init(path string) (*Memtable, error) {
	wal, err := wal.NewWal(path)
	if err != nil {
		return nil, err
	}

	skl := utils.InitSL(0.5, 16)

	return &Memtable{
		wal:  wal,
		data: skl,
	}, nil
}

func (mt *Memtable) Put(key, value string) {}
func (mt *Memtable) Get(key string)        {}
func (mt *Memtable) Delete(key string)     {}
