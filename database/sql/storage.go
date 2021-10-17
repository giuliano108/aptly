package sql

import (
	databasesql "database/sql"
	"errors"

	"github.com/aptly-dev/aptly/database"
)

type storage struct {
	driverName     string
	dataSourceName string
	tableName      string
	db             *databasesql.DB
	putStmt        *databasesql.Stmt
	getStmt        *databasesql.Stmt
}

func (s *storage) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.getStmt.QueryRow(key).Scan(&value)
	if err == databasesql.ErrNoRows {
		err = errors.New("key not found")
	}
	return value, err
}

func (s *storage) Put(key []byte, value []byte) error {
	_, err := s.putStmt.Exec(key, value)
	return err
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
	var err error
	s.db, err = databasesql.Open(s.driverName, s.dataSourceName)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("CREATE TABLE IF NOT EXISTS " + s.tableName + " ( key BLOB NOT NULL PRIMARY KEY, value BLOB );")
	if err != nil {
		return err
	}
	s.putStmt, err = s.db.Prepare("INSERT INTO " + s.tableName + "(key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	s.getStmt, err = s.db.Prepare("SELECT value FROM " + s.tableName + " WHERE key = ?")
	return err
}

func (s *storage) Close() error {
	return s.db.Close()
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
