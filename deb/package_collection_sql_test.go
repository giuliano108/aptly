package deb

import (
	"fmt"

	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"
	"github.com/aptly-dev/aptly/utils"

	. "gopkg.in/check.v1"
)

type PackageCollectionSQLSuite struct {
	collection *PackageCollection
	p          *Package
	db         database.Storage
}

var _ = Suite(&PackageCollectionSQLSuite{})

func (s *PackageCollectionSQLSuite) SetUpTest(c *C) {
	s.p = NewPackageFromControlFile(packageStanza.Copy())

	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)

	s.collection = NewPackageCollection(s.db)
}

func (s *PackageCollectionSQLSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *PackageCollectionSQLSuite) TestUpdate(c *C) {
	// package doesn't exist, update ok
	err := s.collection.Update(s.p)
	c.Assert(err, IsNil)
	res, err := s.collection.ByKey(s.p.Key(""))
	c.Assert(err, IsNil)
	c.Assert(res.Equals(s.p), Equals, true)

	// same package, ok
	p2 := NewPackageFromControlFile(packageStanza.Copy())
	err = s.collection.Update(p2)
	c.Assert(err, IsNil)
	res, err = s.collection.ByKey(p2.Key(""))
	c.Assert(err, IsNil)
	c.Assert(res.Equals(s.p), Equals, true)

	// change some metadata
	p2.Source = "lala"
	err = s.collection.Update(p2)
	c.Assert(err, IsNil)
	res, err = s.collection.ByKey(p2.Key(""))
	c.Assert(err, IsNil)
	c.Assert(res.Equals(s.p), Equals, false)
	c.Assert(res.Equals(p2), Equals, true)
}

func (s *PackageCollectionSQLSuite) TestByKey(c *C) {
	err := s.collection.Update(s.p)
	c.Assert(err, IsNil)

	p2, err := s.collection.ByKey(s.p.Key(""))
	c.Assert(err, IsNil)
	c.Assert(p2.Equals(s.p), Equals, true)

	c.Check(p2.GetDependencies(0), DeepEquals, []string{"libc6 (>= 2.7)", "alien-arena-data (>= 7.40)", "dpkg (>= 1.6)"})
	c.Check(p2.Extra()["Priority"], Equals, "extra")
	c.Check(p2.Files()[0].Filename, Equals, "alien-arena-common_7.40-2_i386.deb")
}

func (s *PackageCollectionSQLSuite) TestByKeyOld0_3(c *C) {
	key := []byte("Pi386 vmware-view-open-client 4.5.0-297975+dfsg-4+b1")
	s.db.Put(key, old0_3Package)

	p, err := s.collection.ByKey(key)
	c.Check(err, IsNil)
	c.Check(p.Name, Equals, "vmware-view-open-client")
	c.Check(p.Version, Equals, "4.5.0-297975+dfsg-4+b1")
	c.Check(p.Architecture, Equals, "i386")
	c.Check(p.Files(), DeepEquals, PackageFiles{
		PackageFile{Filename: "vmware-view-open-client_4.5.0-297975+dfsg-4+b1_i386.deb",
			Checksums: utils.ChecksumInfo{
				Size:   520080,
				MD5:    "9c61b54e2638a18f955a695b9162d6af",
				SHA1:   "5b7c99e64a70f4f509bfa3a674088ff9cef68163",
				SHA256: "4a9e4b2d9b3db13f9a29e522f3ffbb34eee96fc6f34a0647042ab1b5b0f2e04d"}}})
	c.Check(p.GetDependencies(0), DeepEquals, []string{"libatk1.0-0 (>= 1.12.4)", "libboost-signals1.49.0 (>= 1.49.0-1)",
		"libc6 (>= 2.3.6-6~)", "libcairo2 (>= 1.2.4)", "libcurl3 (>= 7.18.0)", "libfontconfig1 (>= 2.8.0)", "libfreetype6 (>= 2.2.1)",
		"libgcc1 (>= 1:4.1.1)", "libgdk-pixbuf2.0-0 (>= 2.22.0)", "libglib2.0-0 (>= 2.24.0)", "libgtk2.0-0 (>= 2.24.0)",
		"libicu48 (>= 4.8-1)", "libpango1.0-0 (>= 1.14.0)", "libssl1.0.0 (>= 1.0.0)", "libstdc++6 (>= 4.6)", "libx11-6",
		"libxml2 (>= 2.7.4)", "rdesktop"})
	c.Check(p.Extra()["Priority"], Equals, "optional")
}

func (s *PackageCollectionSQLSuite) TestAllPackageRefs(c *C) {
	err := s.collection.Update(s.p)
	c.Assert(err, IsNil)

	refs := s.collection.AllPackageRefs()
	c.Check(refs.Len(), Equals, 1)
	c.Check(refs.Refs[0], DeepEquals, s.p.Key(""))
}

func (s *PackageCollectionSQLSuite) TestDeleteByKey(c *C) {
	err := s.collection.Update(s.p)
	c.Assert(err, IsNil)

	_, err = s.db.Get(s.p.Key(""))
	c.Check(err, IsNil)

	_, err = s.db.Get(s.p.Key("xD"))
	c.Check(err, IsNil)

	_, err = s.db.Get(s.p.Key("xE"))
	c.Check(err, IsNil)

	_, err = s.db.Get(s.p.Key("xF"))
	c.Check(err, IsNil)

	err = s.collection.DeleteByKey(s.p.Key(""), s.db)
	c.Check(err, IsNil)

	_, err = s.collection.ByKey(s.p.Key(""))
	c.Check(err, ErrorMatches, "key not found")

	_, err = s.db.Get(s.p.Key(""))
	c.Check(err, ErrorMatches, "key not found")

	_, err = s.db.Get(s.p.Key("xD"))
	c.Check(err, ErrorMatches, "key not found")

	_, err = s.db.Get(s.p.Key("xE"))
	c.Check(err, ErrorMatches, "key not found")

	_, err = s.db.Get(s.p.Key("xF"))
	c.Check(err, ErrorMatches, "key not found")
}
