package sql

import "github.com/aptly-dev/aptly/database"

type batch struct {
	t *transaction
}

func (b *batch) Put(key, value []byte) error {
	return b.t.Put(key, value)
}

func (b *batch) Delete(key []byte) error {
	return b.t.Delete(key)
}

func (b *batch) Write() error {
	return b.t.Commit()
}

// batch should implement database.Batch
var (
	_ database.Batch = &batch{}
)
