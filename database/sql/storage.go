package sql

import (
	databasesql "database/sql"
	"errors"
	"fmt"

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
	var sCopy storage
	var err error
	sCopy = *s
	tableName := fmt.Sprintf("%s-%d", s.tableName, temporaryTableID.Get())
	sCopy.tableName = tableName
	fmt.Println(tableName)

	_, err = s.db.Exec(sCopy.stmts.CreateTableFunc(s.tableName))
	if err != nil {
		return nil, err
	}
	if sCopy.stmts.Pragma != nil {
		_, err = s.db.Exec(sCopy.stmts.Pragma.Stmt)
		if err != nil {
			return nil, err
		}
	}

	return &sCopy, nil
}

func (s *storage) massPrepare() error {
	var err error
	for _, stmt := range []*statement{
		s.stmts.Put,
		s.stmts.Get,
		s.stmts.CountPrefix,
		s.stmts.FetchPrefix,
		s.stmts.KeysPrefix,
		s.stmts.ProcessPrefix,
		s.stmts.Delete,
	} {
		stmt.Prepared, err = s.db.Prepare(stmt.Stmt)
		if err != nil {
			break
		}
	}
	return err
}

func (s *storage) genStatements(tableName string) {
	s.stmts.Put = &statement{Stmt: "INSERT INTO " + tableName + " (key, value) VALUES (?, ?)"}
	s.stmts.Get = &statement{Stmt: "SELECT value FROM " + tableName + " WHERE key = ?"}
	s.stmts.CountPrefix = &statement{Stmt: "SELECT COUNT (key) as count FROM " + tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "'"}
	s.stmts.FetchPrefix = &statement{Stmt: "SELECT value FROM " + tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key"}
	s.stmts.KeysPrefix = &statement{Stmt: "SELECT key FROM " + tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key"}
	s.stmts.ProcessPrefix = &statement{Stmt: "SELECT key, value FROM " + tableName + " WHERE KEY LIKE ? ESCAPE '" + string(s.escapeCharacter) + "' ORDER BY key"}
	s.stmts.Delete = &statement{Stmt: "DELETE FROM " + tableName + " WHERE key = ?"}
}

func (s *storage) Open() error {
	var err error
	s.db, err = databasesql.Open(s.driverName, s.dataSourceName)
	if err != nil {
		return err
	}

	s.escapeCharacter = []byte("\\")

	createTableFunc := func(tableName string) string {
		return "CREATE TABLE IF NOT EXISTS " + tableName + " ( key BLOB NOT NULL PRIMARY KEY, value BLOB )"
	}
	pragmaStmt := &statement{Stmt: "PRAGMA case_sensitive_like = true"}

	_, err = s.db.Exec(createTableFunc(s.tableName))
	if err != nil {
		return err
	}
	if pragmaStmt != nil {
		_, err = s.db.Exec(pragmaStmt.Stmt)
		if err != nil {
			return err
		}
	}

	s.genStatements(s.tableName)
	err = s.massPrepare()
	if err != nil {
		return err
	}

	s.stmts.CreateTableFunc = createTableFunc
	s.stmts.Pragma = pragmaStmt

	return nil
}

func (s *storage) Close() error {
	return s.db.Close()
}

// Does nothing, we can't "compact" a SQL database via a library call
func (s *storage) CompactDB() error {
	//TODO: log that this is a noop
	return nil
}

func (s *storage) Drop() error {
	panic("not implemented") // TODO: Implement
}

// Check interface
var (
	_ database.Storage = &storage{}
)
