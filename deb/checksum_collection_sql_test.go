package deb

import (
	"fmt"

	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"
	"github.com/aptly-dev/aptly/utils"

	. "gopkg.in/check.v1"
)

type ChecksumCollectionSQLSuite struct {
	collection *ChecksumCollection
	c          utils.ChecksumInfo
	db         database.Storage
}

var _ = Suite(&ChecksumCollectionSQLSuite{})

func (s *ChecksumCollectionSQLSuite) SetUpTest(c *C) {
	s.c = utils.ChecksumInfo{
		Size:   124,
		MD5:    "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		SHA1:   "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}

	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)
	s.collection = NewChecksumCollection(s.db)
}

func (s *ChecksumCollectionSQLSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *ChecksumCollectionSQLSuite) TestFlow(c *C) {
	// checksum not stored
	checksum, err := s.collection.Get("some/path")
	c.Assert(err, IsNil)
	c.Check(checksum, IsNil)

	// store checksum
	err = s.collection.Update("some/path", &s.c)
	c.Assert(err, IsNil)

	// load it back
	checksum, err = s.collection.Get("some/path")
	c.Assert(err, IsNil)
	c.Check(*checksum, DeepEquals, s.c)
}
