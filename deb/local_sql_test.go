package deb

import (
	"errors"
	"fmt"

	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"

	. "gopkg.in/check.v1"
)

type LocalRepoSQLSuite struct {
	db      database.Storage
	list    *PackageList
	reflist *PackageRefList
	repo    *LocalRepo
}

var _ = Suite(&LocalRepoSQLSuite{})

func (s *LocalRepoSQLSuite) SetUpTest(c *C) {
	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)
	s.list = NewPackageList()
	s.list.Add(&Package{Name: "lib", Version: "1.7", Architecture: "i386"})
	s.list.Add(&Package{Name: "app", Version: "1.9", Architecture: "amd64"})

	s.reflist = NewPackageRefListFromPackageList(s.list)

	s.repo = NewLocalRepo("lrepo", "Super repo")
	s.repo.packageRefs = s.reflist
}

func (s *LocalRepoSQLSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *LocalRepoSQLSuite) TestString(c *C) {
	c.Check(NewLocalRepo("lrepo", "My first repo").String(), Equals, "[lrepo]: My first repo")
	c.Check(NewLocalRepo("lrepo2", "").String(), Equals, "[lrepo2]")
}

func (s *LocalRepoSQLSuite) TestNumPackages(c *C) {
	c.Check(NewLocalRepo("lrepo", "My first repo").NumPackages(), Equals, 0)
	c.Check(s.repo.NumPackages(), Equals, 2)
}

func (s *LocalRepoSQLSuite) TestRefList(c *C) {
	c.Check(NewLocalRepo("lrepo", "My first repo").RefList(), IsNil)
	c.Check(s.repo.RefList(), Equals, s.reflist)
}

func (s *LocalRepoSQLSuite) TestUpdateRefList(c *C) {
	s.repo.UpdateRefList(nil)
	c.Check(s.repo.RefList(), IsNil)
}

func (s *LocalRepoSQLSuite) TestEncodeDecode(c *C) {
	repo := &LocalRepo{}
	err := repo.Decode(s.repo.Encode())
	c.Assert(err, IsNil)

	c.Check(repo.Name, Equals, s.repo.Name)
	c.Check(repo.Comment, Equals, s.repo.Comment)
}

func (s *LocalRepoSQLSuite) TestKey(c *C) {
	c.Assert(len(s.repo.Key()), Equals, 37)
	c.Assert(s.repo.Key()[0], Equals, byte('L'))
}

func (s *LocalRepoSQLSuite) TestRefKey(c *C) {
	c.Assert(len(s.repo.RefKey()), Equals, 37)
	c.Assert(s.repo.RefKey()[0], Equals, byte('E'))
	c.Assert(s.repo.RefKey()[1:], DeepEquals, s.repo.Key()[1:])
}

type LocalRepoCollectionSQLSuite struct {
	db         database.Storage
	collection *LocalRepoCollection
	list       *PackageList
	reflist    *PackageRefList
}

var _ = Suite(&LocalRepoCollectionSQLSuite{})

func (s *LocalRepoCollectionSQLSuite) SetUpTest(c *C) {
	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)
	s.collection = NewLocalRepoCollection(s.db)

	s.list = NewPackageList()
	s.list.Add(&Package{Name: "lib", Version: "1.7", Architecture: "i386"})
	s.list.Add(&Package{Name: "app", Version: "1.9", Architecture: "amd64"})

	s.reflist = NewPackageRefListFromPackageList(s.list)
}

func (s *LocalRepoCollectionSQLSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *LocalRepoCollectionSQLSuite) TestAddByName(c *C) {
	_, err := s.collection.ByName("local1")
	c.Assert(err, ErrorMatches, "*.not found")

	repo := NewLocalRepo("local1", "Comment 1")
	c.Assert(s.collection.Add(repo), IsNil)
	c.Assert(s.collection.Add(repo), ErrorMatches, ".*already exists")

	r, err := s.collection.ByName("local1")
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())

	collection := NewLocalRepoCollection(s.db)
	r, err = collection.ByName("local1")
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())
}

func (s *LocalRepoCollectionSQLSuite) TestByUUID(c *C) {
	_, err := s.collection.ByUUID("some-uuid")
	c.Assert(err, ErrorMatches, "*.not found")

	repo := NewLocalRepo("local1", "Comment 1")
	c.Assert(s.collection.Add(repo), IsNil)

	r, err := s.collection.ByUUID(repo.UUID)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, repo)

	collection := NewLocalRepoCollection(s.db)
	r, err = collection.ByUUID(repo.UUID)
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())
}

func (s *LocalRepoCollectionSQLSuite) TestUpdateLoadComplete(c *C) {
	repo := NewLocalRepo("local1", "Comment 1")
	c.Assert(s.collection.Update(repo), IsNil)

	collection := NewLocalRepoCollection(s.db)
	r, err := collection.ByName("local1")
	c.Assert(err, IsNil)
	c.Assert(r.packageRefs, IsNil)

	repo.packageRefs = s.reflist
	c.Assert(s.collection.Update(repo), IsNil)

	collection = NewLocalRepoCollection(s.db)
	r, err = collection.ByName("local1")
	c.Assert(err, IsNil)
	c.Assert(r.packageRefs, IsNil)
	c.Assert(r.NumPackages(), Equals, 0)
	c.Assert(s.collection.LoadComplete(r), IsNil)
	c.Assert(r.NumPackages(), Equals, 2)
}

func (s *LocalRepoCollectionSQLSuite) TestForEachAndLen(c *C) {
	repo := NewLocalRepo("local1", "Comment 1")
	s.collection.Add(repo)

	count := 0
	err := s.collection.ForEach(func(*LocalRepo) error {
		count++
		return nil
	})
	c.Assert(count, Equals, 1)
	c.Assert(err, IsNil)

	c.Check(s.collection.Len(), Equals, 1)

	e := errors.New("c")

	err = s.collection.ForEach(func(*LocalRepo) error {
		return e
	})
	c.Assert(err, Equals, e)
}

func (s *LocalRepoCollectionSQLSuite) TestDrop(c *C) {
	repo1 := NewLocalRepo("local1", "Comment 1")
	s.collection.Add(repo1)

	repo2 := NewLocalRepo("local2", "Comment 2")
	s.collection.Add(repo2)

	r1, _ := s.collection.ByUUID(repo1.UUID)
	c.Check(r1, Equals, repo1)

	err := s.collection.Drop(repo1)
	c.Check(err, IsNil)

	_, err = s.collection.ByUUID(repo1.UUID)
	c.Check(err, ErrorMatches, "local repo .* not found")

	collection := NewLocalRepoCollection(s.db)
	_, err = collection.ByName("local1")
	c.Check(err, ErrorMatches, "local repo .* not found")

	r2, _ := collection.ByName("local2")
	c.Check(r2.String(), Equals, repo2.String())

	c.Check(s.collection.Drop(repo1), ErrorMatches, "local repo not found")
}
