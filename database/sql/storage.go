package sql

import (
	databasesql "database/sql"
	"errors"

	"github.com/aptly-dev/aptly/database"
)

type storage struct {
	driverName        string
	dataSourceName    string
	tableName         string
	escapeCharacter   []byte
	db                *databasesql.DB
	putStmt           *databasesql.Stmt
	getStmt           *databasesql.Stmt
	countPrefixStmt   *databasesql.Stmt
	fetchPrefixStmt   *databasesql.Stmt
	keysPrefixStmt    *databasesql.Stmt
	processPrefixStmt *databasesql.Stmt
	deleteStmt        *databasesql.Stmt
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
	_, err := s.deleteStmt.Exec(key)
	return err
}

// FetchByPrefix returns all values with keys that start with prefix
func (s *storage) FetchByPrefix(prefix []byte) [][]byte {
	values := make([][]byte, 0, 20)
	rows, err := s.fetchPrefixStmt.Query(PrefixPattern(prefix, s.escapeCharacter))
	if err != nil {
		panic("error")
	}
	defer rows.Close()
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			panic("error")
		}
		values = append(values, value)
	}
	if err = rows.Err(); err != nil {
		panic("error")
	}
	return values
}

// HasPrefix checks whether it can find any key with given prefix and returns true if one exists
func (s *storage) HasPrefix(prefix []byte) bool {
	var count int
	err := s.countPrefixStmt.QueryRow(PrefixPattern(prefix, s.escapeCharacter)).Scan(&count)
	if err != nil {
		panic("error")
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

// ProcessByPrefix iterates through all entries where key starts with prefix and calls
// StorageProcessor on key value pair
func (s *storage) ProcessByPrefix(prefix []byte, proc database.StorageProcessor) error {
	rows, err := s.processPrefixStmt.Query(PrefixPattern(prefix, s.escapeCharacter))
	if err != nil {
		panic("error")
	}
	defer rows.Close()
	for rows.Next() {
		var key []byte
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			panic("error")
		}
		err := proc(key, value)
		if err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		panic("error")
	}

	return nil
}

// KeysByPrefix returns all keys that start with prefix
func (s *storage) KeysByPrefix(prefix []byte) [][]byte {
	keys := make([][]byte, 0, 20)
	rows, err := s.keysPrefixStmt.Query(PrefixPattern(prefix, s.escapeCharacter))
	if err != nil {
		panic("error")
	}
	defer rows.Close()
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			panic("error")
		}
		keys = append(keys, key)
	}
	if err = rows.Err(); err != nil {
		panic("error")
	}
	return keys
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
	s.escapeCharacter = []byte("\\")
	if err != nil {
		return err
	}
	_, err = s.db.Exec("CREATE TABLE IF NOT EXISTS " + s.tableName + " ( key BLOB NOT NULL PRIMARY KEY, value BLOB )")
	_, err = s.db.Exec("PRAGMA case_sensitive_like = true")
	if err != nil {
		return err
	}
	s.putStmt, err = s.db.Prepare("INSERT INTO " + s.tableName + "(key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	s.getStmt, err = s.db.Prepare("SELECT value FROM " + s.tableName + " WHERE key = ?")
	if err != nil {
		return err
	}
	s.countPrefixStmt, err = s.db.Prepare("SELECT COUNT (key) as count FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "'")
	if err != nil {
		return err
	}
	s.fetchPrefixStmt, err = s.db.Prepare("SELECT value FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	s.keysPrefixStmt, err = s.db.Prepare("SELECT key FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	s.processPrefixStmt, err = s.db.Prepare("SELECT key, value FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	s.deleteStmt, err = s.db.Prepare("DELETE FROM " + s.tableName + " WHERE key = ?")
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
