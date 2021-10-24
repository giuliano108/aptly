package sql

import (
	databasesql "database/sql"
)

type statement struct {
	Stmt     string
	Prepared *databasesql.Stmt
}

type statements struct {
	Put           *statement
	Get           *statement
	CountPrefix   *statement
	FetchPrefix   *statement
	KeysPrefix    *statement
	ProcessPrefix *statement
	Delete        *statement
}
