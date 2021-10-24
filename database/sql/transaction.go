package sql

import (
	databasesql "database/sql"
	"errors"

	"github.com/aptly-dev/aptly/database"
)

type transaction struct {
	t     *databasesql.Tx
	stmts statements
}

// Get implements database.Reader interface.
func (t *transaction) Get(key []byte) ([]byte, error) {
	var value []byte
	err := t.t.Stmt(t.stmts.Get.Prepared).QueryRow(key).Scan(&value)
	if err == databasesql.ErrNoRows {
		err = errors.New("key not found")
	}
	return value, err
}

// Put implements database.Writer interface.
func (t *transaction) Put(key, value []byte) error {
	_, err := t.t.Stmt(t.stmts.Put.Prepared).Exec(key, value)
	return err
}

// Delete implements database.Writer interface.
func (t *transaction) Delete(key []byte) error {
	_, err := t.t.Stmt(t.stmts.Delete.Prepared).Exec(key)
	return err
}

// Commit finalizes transaction and commits changes to the stable storage.
func (t *transaction) Commit() error {
	return t.t.Commit()
}

// Discard any transaction changes.
//
// Discard is safe to call after Commit(), it would be no-op
func (t *transaction) Discard() {
	err := t.t.Rollback()
	if err != nil {
		panic("error")
	}
}

// transaction should implement database.Transaction
var _ database.Transaction = &transaction{}
