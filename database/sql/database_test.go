package sql_test

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"

	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/goleveldb"
	"github.com/aptly-dev/aptly/database/sql"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SQLSuite struct {
	path           string
	driverName     string
	dataSourceName string
	tableName      string
	db             database.Storage
}

var _ = Suite(&SQLSuite{})

func (s *SQLSuite) SetUpTest(c *C) {
	var err error

	s.path = c.MkDir()
	s.driverName = "sqlite3"
	s.dataSourceName = fmt.Sprintf("file:%s/sql_test.db", s.path)
	s.tableName = "testtable"
	s.db, err = sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TearDownTest(c *C) {
	err := s.db.Close()
	c.Assert(err, IsNil)
}

//
// These tests are copied 1:1 from goleveldb_test
//

func (s *SQLSuite) TestRecoverDB(c *C) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	err := s.db.Put(key, value)
	c.Check(err, IsNil)

	err = s.db.Close()
	c.Check(err, IsNil)

	err = goleveldb.RecoverDB(s.path)
	c.Check(err, IsNil)

	s.db, err = sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	c.Check(err, IsNil)

	result, err := s.db.Get(key)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, value)
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

func (s *SQLSuite) TestTemporaryDelete(c *C) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	temp, err := s.db.CreateTemporary()
	c.Assert(err, IsNil)

	c.Check(s.db.HasPrefix([]byte(nil)), Equals, true)
	c.Check(temp.HasPrefix([]byte(nil)), Equals, false)

	err = temp.Put(key, value)
	c.Assert(err, IsNil)
	c.Check(temp.HasPrefix([]byte(nil)), Equals, true)

	c.Assert(temp.Close(), IsNil)
	c.Assert(temp.Drop(), IsNil)
}

func (s *SQLSuite) TestDelete(c *C) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	err = s.db.Delete(key)
	c.Assert(err, IsNil)

	_, err = s.db.Get(key)
	c.Assert(err, ErrorMatches, "key not found")

	err = s.db.Delete(key)
	c.Assert(err, IsNil)
}

func (s *SQLSuite) TestByPrefix(c *C) {
	c.Check(s.db.FetchByPrefix([]byte{0x80}), DeepEquals, [][]byte{})

	s.db.Put([]byte{0x80, 0x01}, []byte{0x01})
	s.db.Put([]byte{0x80, 0x03}, []byte{0x03})
	s.db.Put([]byte{0x80, 0x02}, []byte{0x02})
	c.Check(s.db.FetchByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x01}, {0x02}, {0x03}})
	c.Check(s.db.KeysByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x80, 0x01}, {0x80, 0x02}, {0x80, 0x03}})

	s.db.Put([]byte{0x90, 0x01}, []byte{0x04})
	c.Check(s.db.FetchByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x01}, {0x02}, {0x03}})
	c.Check(s.db.KeysByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x80, 0x01}, {0x80, 0x02}, {0x80, 0x03}})

	s.db.Put([]byte{0x00, 0x01}, []byte{0x05})
	c.Check(s.db.FetchByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x01}, {0x02}, {0x03}})
	c.Check(s.db.KeysByPrefix([]byte{0x80}), DeepEquals, [][]byte{{0x80, 0x01}, {0x80, 0x02}, {0x80, 0x03}})

	keys := [][]byte{}
	values := [][]byte{}

	c.Check(s.db.ProcessByPrefix([]byte{0x80}, func(k, v []byte) error {
		keys = append(keys, append([]byte(nil), k...))
		values = append(values, append([]byte(nil), v...))
		return nil
	}), IsNil)

	c.Check(values, DeepEquals, [][]byte{{0x01}, {0x02}, {0x03}})
	c.Check(keys, DeepEquals, [][]byte{{0x80, 0x01}, {0x80, 0x02}, {0x80, 0x03}})

	c.Check(s.db.ProcessByPrefix([]byte{0x80}, func(k, v []byte) error {
		return database.ErrNotFound
	}), Equals, database.ErrNotFound)

	c.Check(s.db.ProcessByPrefix([]byte{0xa0}, func(k, v []byte) error {
		return database.ErrNotFound
	}), IsNil)

	c.Check(s.db.FetchByPrefix([]byte{0xa0}), DeepEquals, [][]byte{})
	c.Check(s.db.KeysByPrefix([]byte{0xa0}), DeepEquals, [][]byte{})
}

func (s *SQLSuite) TestHasPrefix(c *C) {
	c.Check(s.db.HasPrefix([]byte(nil)), Equals, false)
	c.Check(s.db.HasPrefix([]byte{0x80}), Equals, false)

	s.db.Put([]byte{0x80, 0x01}, []byte{0x01})

	c.Check(s.db.HasPrefix([]byte(nil)), Equals, true)
	c.Check(s.db.HasPrefix([]byte{0x80}), Equals, true)
	c.Check(s.db.HasPrefix([]byte{0x79}), Equals, false)
}

func (s *SQLSuite) TestBatch(c *C) {
	var (
		key    = []byte("key")
		key2   = []byte("key2")
		value  = []byte("value")
		value2 = []byte("value2")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	batch := s.db.CreateBatch()
	batch.Put(key2, value2)
	batch.Delete(key)

	v, err := s.db.Get(key)
	c.Check(err, IsNil)
	c.Check(v, DeepEquals, value)

	_, err = s.db.Get(key2)
	c.Check(err, ErrorMatches, "key not found")

	err = batch.Write()
	c.Check(err, IsNil)

	v2, err := s.db.Get(key2)
	c.Check(err, IsNil)
	c.Check(v2, DeepEquals, value2)

	_, err = s.db.Get(key)
	c.Check(err, ErrorMatches, "key not found")
}

func (s *SQLSuite) TestTransactionCommit(c *C) {
	var (
		key    = []byte("key")
		key2   = []byte("key2")
		value  = []byte("value")
		value2 = []byte("value2")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	transaction, err := s.db.OpenTransaction()
	c.Assert(err, IsNil)
	transaction.Put(key2, value2)
	transaction.Delete(key)

	v, err := s.db.Get(key)
	c.Check(err, IsNil)
	c.Check(v, DeepEquals, value)

	_, err = s.db.Get(key2)
	c.Check(err, ErrorMatches, "key not found")

	v2, err := transaction.Get(key2)
	c.Check(err, IsNil)
	c.Check(v2, DeepEquals, value2)

	_, err = transaction.Get(key)
	c.Check(err, ErrorMatches, "key not found")

	err = transaction.Commit()
	c.Check(err, IsNil)

	v2, err = s.db.Get(key2)
	c.Check(err, IsNil)
	c.Check(v2, DeepEquals, value2)

	_, err = s.db.Get(key)
	c.Check(err, ErrorMatches, "key not found")
}

func (s *SQLSuite) TestTransactionDiscard(c *C) {
	var (
		key    = []byte("key")
		key2   = []byte("key2")
		value  = []byte("value")
		value2 = []byte("value2")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	transaction, err := s.db.OpenTransaction()
	c.Assert(err, IsNil)
	transaction.Put(key2, value2)
	transaction.Delete(key)

	v, err := s.db.Get(key)
	c.Check(err, IsNil)
	c.Check(v, DeepEquals, value)

	_, err = s.db.Get(key2)
	c.Check(err, ErrorMatches, "key not found")

	v2, err := transaction.Get(key2)
	c.Check(err, IsNil)
	c.Check(v2, DeepEquals, value2)

	_, err = transaction.Get(key)
	c.Check(err, ErrorMatches, "key not found")

	transaction.Discard()

	v, err = s.db.Get(key)
	c.Check(err, IsNil)
	c.Check(v, DeepEquals, value)

	_, err = s.db.Get(key2)
	c.Check(err, ErrorMatches, "key not found")
}

func (s *SQLSuite) TestCompactDB(c *C) {
	s.db.Put([]byte{0x80, 0x01}, []byte{0x01})
	s.db.Put([]byte{0x80, 0x03}, []byte{0x03})
	s.db.Put([]byte{0x80, 0x02}, []byte{0x02})

	c.Check(s.db.CompactDB(), IsNil)
}

func (s *SQLSuite) TestReOpen(c *C) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)

	err := s.db.Put(key, value)
	c.Assert(err, IsNil)

	err = s.db.Close()
	c.Assert(err, IsNil)

	err = s.db.Open()
	c.Assert(err, IsNil)

	result, err := s.db.Get(key)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, value)
}

//
// SQL specific tests
//

// Aptly uses LevelDB's default comparator, which results in keys being ordered lexicographically.
// See: https://github.com/google/leveldb/blob/master/doc/index.md#comparators
// This property is already exercised in `TestByPrefix`, but we also do it here explicitly. It is
// achieved using an `ORDER BY` SQL caluse.
func (s *SQLSuite) TestOrdering(c *C) {
	c.Check(s.db.FetchByPrefix([]byte{0xF0}), DeepEquals, [][]byte{})

	s.db.Put([]byte{0xF0, 0x01}, []byte{0x01})
	s.db.Put([]byte{0xF0, 0x03}, []byte{0x03})
	s.db.Put([]byte{0xF0, 0x02}, []byte{0x02})
	c.Check(s.db.FetchByPrefix([]byte{0xF0}), DeepEquals, [][]byte{{0x01}, {0x02}, {0x03}})
}

func (s *SQLSuite) TestEscapeLikeWildcardCharacters(c *C) {
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("foo"), []byte("\\")), DeepEquals, []byte("foo"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("foo%"), []byte("\\")), DeepEquals, []byte("foo\\%"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("f_o%"), []byte("\\")), DeepEquals, []byte("f\\_o\\%"))
	c.Check(sql.EscapeLikeWildcardCharacters([]byte("\\%foo%"), []byte("\\")), DeepEquals, []byte("\\\\\\%foo\\%"))
}

// Different SQL DBs have different ways to make `LIKE 'prefix%'` queries
// case sensitive (by default, they are not).
func (s *SQLSuite) TestCaseSensitivity(c *C) {
	var (
		err   error
		value = []byte("value")
	)

	c.Assert(err, IsNil)

	// Put/Get are case sensitive.
	err = s.db.Put([]byte("SOMEKEY"), value)
	c.Assert(err, IsNil)

	_, err = s.db.Get([]byte("somekey"))
	c.Assert(err, ErrorMatches, "key not found")

	// Prefixed operations are case sensitive too.
	_ = s.db.Put([]byte("KEYUPPER"), value)
	_ = s.db.Put([]byte("keylower"), value)
	c.Check(s.db.KeysByPrefix([]byte("KEY")), DeepEquals, [][]byte{[]byte("KEYUPPER")})

	// same test, in a transaction (things like "PRAGMA case_sensitive_like = true" might
	// need to be repeated after a BEGIN)
	transaction, err := s.db.OpenTransaction()
	c.Assert(err, IsNil)

	// Put/Get are case sensitive.
	err = transaction.Put([]byte("TSOMEKEY"), value)
	c.Assert(err, IsNil)

	_, err = transaction.Get([]byte("tsomekey"))
	c.Assert(err, ErrorMatches, "key not found")

	// Transactions don't support Prefixed operations, so no KeysByPrefix test needed
}

// Two subsequent Put() s, for the same key, update the value
func (s *SQLSuite) TestUniqueConstraint(c *C) {
	var key = []byte("key")
	var err error
	err = s.db.Put(key, []byte("value1"))
	c.Assert(err, IsNil)
	err = s.db.Put(key, []byte("value2"))
	c.Assert(err, IsNil)
	result, err := s.db.Get(key)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []byte("value2"))
}

// Similar to TestGetPut but stricter about the expected errors
func (s *SQLSuite) TestGetPutStrict(c *C) {
	var (
		key = []byte("key")
	)

	_, err := s.db.Get(key)
	c.Assert(err, ErrorMatches, "key not found")
	// "plain" TestGetPut doesn't have this assertion
	// https://github.com/aptly-dev/aptly/blob/cbf0416d7e5070f58d0b40fc1be3e771b0baacf4/deb/local.go#L186
	c.Assert(err == database.ErrNotFound, Equals, true)

	// same test, in a transaction
	transaction, err := s.db.OpenTransaction()
	c.Assert(err, IsNil)
	_, err = transaction.Get(key)
	c.Assert(err == database.ErrNotFound, Equals, true)
}
