package sql

import (
	databasesql "database/sql"

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
		return nil, database.ErrNotFound
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
	// The code can call Discard() after Commit(), so we have to account for that. See f.e.:
	// https://github.com/aptly-dev/aptly/blob/ab2f5420c61749ac601e0d3cb245fb2362010aa8/deb/package_collection.go#L210
	if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
		panic(err)
	}
}

// transaction should implement database.Transaction
var _ database.Transaction = &transaction{}
