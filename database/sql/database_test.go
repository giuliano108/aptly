package sql_test

import (
	"testing"

	. "gopkg.in/check.v1"

	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SQLSuite struct {
	driverName     string
	dataSourceName string
	db             database.Storage
}

var _ = Suite(&SQLSuite{})

func (s *SQLSuite) SetUpTest(c *C) {
	var err error

	s.driverName = "sqlite3"
	s.dataSourceName = ":memory:"
	s.db, err = sql.NewOpenDB(s.driverName, s.dataSourceName)
	c.Assert(err, IsNil)
}
