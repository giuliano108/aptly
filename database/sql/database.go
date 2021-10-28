package sql

import (
	"github.com/aptly-dev/aptly/database"
	_ "github.com/mattn/go-sqlite3"
)

// NewDB creates new instance of DB, but doesn't open it (yet)
func NewDB(driverName string, dataSourceName string, tableName string) (database.Storage, error) {
	return &storage{
		driverName:     driverName,
		dataSourceName: dataSourceName,
		tableName:      tableName,
	}, nil
}

// NewOpenDB creates new instance of DB and opens it
func NewOpenDB(driverName string, dataSourceName string, tableName string) (database.Storage, error) {
	s, err := NewDB(driverName, dataSourceName, tableName)
	if err != nil {
		return nil, err
	}

	return s, s.Open()
}

// Does nothing, we can't "recover" a SQL database via a library call
func RecoverDB(path string) error {
	//TODO: log that this is a noop
	return nil
}
