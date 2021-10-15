package sql

import (
	"testing"

	. "gopkg.in/check.v1"

	"github.com/aptly-dev/aptly/database"
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
	s.db, err = NewOpenDB(s.driverName, s.dataSourceName)
	c.Assert(err, IsNil)

	internalDB := s.db.(*storage).db
	_, err = internalDB.Exec("CREATE TABLE blah ( key BLOB NOT NULL, value BLOB NOT NULL);")
	c.Assert(err, IsNil)
	_, err = internalDB.Exec("CREATE UNIQUE INDEX idx_blah ON blah (key);")
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TearDownTest(c *C) {
	err := s.db.Close()
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TestBlah(c *C) {
	c.Check(true, Equals, true)
}
