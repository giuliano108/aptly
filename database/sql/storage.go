package sql

import (
	databasesql "database/sql"

	"github.com/aptly-dev/aptly/database"
)

type storage struct {
	driverName     string
	dataSourceName string
	db             *databasesql.DB
}

func (s *storage) Get(key []byte) ([]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (s *storage) Put(key []byte, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) Delete(key []byte) error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) HasPrefix(prefix []byte) bool {
	panic("not implemented") // TODO: Implement
}

func (s *storage) ProcessByPrefix(prefix []byte, proc database.StorageProcessor) error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) KeysByPrefix(prefix []byte) [][]byte {
	panic("not implemented") // TODO: Implement
}

func (s *storage) FetchByPrefix(prefix []byte) [][]byte {
	panic("not implemented") // TODO: Implement
}

func (s *storage) CreateBatch() database.Batch {
	panic("not implemented") // TODO: Implement
}

func (s *storage) OpenTransaction() (database.Transaction, error) {
	panic("not implemented") // TODO: Implement
}

func (s *storage) CreateTemporary() (database.Storage, error) {
	panic("not implemented") // TODO: Implement
}

func (s *storage) Open() error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) Close() error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) CompactDB() error {
	panic("not implemented") // TODO: Implement
}

func (s *storage) Drop() error {
	panic("not implemented") // TODO: Implement
}

// Check interface
var (
	_ database.Storage = &storage{}
)
