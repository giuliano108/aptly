package sql

import (
	databasesql "database/sql"
	"errors"

	"github.com/aptly-dev/aptly/database"
)

type storage struct {
	driverName      string
	dataSourceName  string
	tableName       string
	escapeCharacter []byte
	db              *databasesql.DB
	stmts           statements
}

func (s *storage) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.stmts.Get.Prepared.QueryRow(key).Scan(&value)
	if err == databasesql.ErrNoRows {
		err = errors.New("key not found")
	}
	return value, err
}

func (s *storage) Put(key []byte, value []byte) error {
	_, err := s.stmts.Put.Prepared.Exec(key, value)
	return err
}

func (s *storage) Delete(key []byte) error {
	_, err := s.stmts.Delete.Prepared.Exec(key)
	return err
}

// FetchByPrefix returns all values with keys that start with prefix
func (s *storage) FetchByPrefix(prefix []byte) [][]byte {
	values := make([][]byte, 0, 20)
	rows, err := s.stmts.FetchPrefix.Prepared.Query(PrefixPattern(prefix, s.escapeCharacter))
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
	err := s.stmts.CountPrefix.Prepared.QueryRow(PrefixPattern(prefix, s.escapeCharacter)).Scan(&count)
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
	rows, err := s.stmts.ProcessPrefix.Prepared.Query(PrefixPattern(prefix, s.escapeCharacter))
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
	rows, err := s.stmts.KeysPrefix.Prepared.Query(PrefixPattern(prefix, s.escapeCharacter))
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
	t, err := s.db.Begin()
	if err != nil {
		panic("error")
	}
	return &batch{t: &transaction{t: t, stmts: s.stmts}}
}

func (s *storage) OpenTransaction() (database.Transaction, error) {
	t, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	return &transaction{t: t, stmts: s.stmts}, nil
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

	putStmt, err := s.NewStatement("INSERT INTO " + s.tableName + "(key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	getStmt, err := s.NewStatement("SELECT value FROM " + s.tableName + " WHERE key = ?")
	if err != nil {
		return err
	}
	countPrefixStmt, err := s.NewStatement("SELECT COUNT (key) as count FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "'")
	if err != nil {
		return err
	}
	fetchPrefixStmt, err := s.NewStatement("SELECT value FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	keysPrefixStmt, err := s.NewStatement("SELECT key FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	processPrefixStmt, err := s.NewStatement("SELECT key, value FROM " + s.tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key")
	if err != nil {
		return err
	}
	deleteStmt, err := s.NewStatement("DELETE FROM " + s.tableName + " WHERE key = ?")
	if err != nil {
		return err
	}

	s.stmts = statements{
		Put:           putStmt,
		Get:           getStmt,
		CountPrefix:   countPrefixStmt,
		FetchPrefix:   fetchPrefixStmt,
		KeysPrefix:    keysPrefixStmt,
		ProcessPrefix: processPrefixStmt,
		Delete:        deleteStmt,
	}

	return nil
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

func (s *storage) NewStatement(stmt string) (*statement, error) {
	prepared, err := s.db.Prepare(stmt)
	if err != nil {
		return nil, err
	} else {
		return &statement{Stmt: stmt, Prepared: prepared}, nil
	}
}

// Check interface
var (
	_ database.Storage = &storage{}
)
