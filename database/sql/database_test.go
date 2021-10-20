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
	tableName      string
	db             database.Storage
}

var _ = Suite(&SQLSuite{})

func (s *SQLSuite) SetUpTest(c *C) {
	var err error

	s.driverName = "sqlite3"
	s.dataSourceName = ":memory:"
	s.tableName = "testtable"
	s.db, err = sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TearDownTest(c *C) {
	err := s.db.Close()
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TestGetPut(c *C) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	_, err := s.db.Get(key)
	c.Assert(err, ErrorMatches, "key not found")

	err = s.db.Put(key, value)
	c.Assert(err, IsNil)

	result, err := s.db.Get(key)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, value)
}

func (s *SQLSuite) TestHasPrefix(c *C) {
	c.Check(s.db.HasPrefix([]byte(nil)), Equals, false)
	c.Check(s.db.HasPrefix([]byte{0x80}), Equals, false)

	s.db.Put([]byte{0x80, 0x01}, []byte{0x01})

	c.Check(s.db.HasPrefix([]byte(nil)), Equals, true)
	c.Check(s.db.HasPrefix([]byte{0x80}), Equals, true)
	c.Check(s.db.HasPrefix([]byte{0x79}), Equals, false)
}

func (s *SQLSuite) TestEscapeLikeWildcardCharacters(c *C) {
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("foo"), []byte("\\")), DeepEquals, []byte("foo"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("foo%"), []byte("\\")), DeepEquals, []byte("foo\\%"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("f_o%"), []byte("\\")), DeepEquals, []byte("f\\_o\\%"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("\\%foo%"), []byte("\\")), DeepEquals, []byte("\\\\\\%foo\\%"))
}

/*
// Test the quirks of using a SQL DB as a KV store.
// These tests run against their own DB table.
func (s *SQLSuite) TestSQLAsAKVStore(c *C) {
	var (
		err   error
		value = []byte("value")
	)

	db, err := sql.NewOpenDB(s.driverName, s.dataSourceName, "testtablequirks")
	c.Assert(err, IsNil)

	// Put/Get are case sensitive.
	err = s.db.Put([]byte("UPPERCASE"), value)
	c.Assert(err, IsNil)

	_, err = s.db.Get([]byte("uppercase"))
	c.Assert(err, ErrorMatches, "key not found")

	// Prefixed operations are case sensitive too.
	// Different SQL DBs have different ways to make `LIKE 'prefix%'` queries
	// case sensitive (by default, they are not).

	// Keys containing SQL `LIKE` wildcard characters are handled correctly

	db.Close()
}
*/
