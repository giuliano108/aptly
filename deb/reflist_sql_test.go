package deb

import (
	"fmt"

	"github.com/aptly-dev/aptly/database/sql"

	. "gopkg.in/check.v1"
)

type PackageRefListSQLSuite struct {
	// Simple list with "real" packages from stanzas
	list                   *PackageList
	p1, p2, p3, p4, p5, p6 *Package
	driverName             string
	dataSourceName         string
	tableName              string
}

var _ = Suite(&PackageRefListSQLSuite{})

func (s *PackageRefListSQLSuite) SetUpTest(c *C) {
	s.list = NewPackageList()

	s.p1 = NewPackageFromControlFile(packageStanza.Copy())
	s.p2 = NewPackageFromControlFile(packageStanza.Copy())
	stanza := packageStanza.Copy()
	stanza["Package"] = "mars-invaders"
	s.p3 = NewPackageFromControlFile(stanza)
	stanza = packageStanza.Copy()
	stanza["Source"] = "unknown-planet"
	s.p4 = NewPackageFromControlFile(stanza)
	stanza = packageStanza.Copy()
	stanza["Package"] = "lonely-strangers"
	s.p5 = NewPackageFromControlFile(stanza)
	stanza = packageStanza.Copy()
	stanza["Version"] = "99.1"
	s.p6 = NewPackageFromControlFile(stanza)

	s.driverName = "sqlite3"
	s.dataSourceName = fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	s.tableName = "testtable"
}

func (s *PackageRefListSQLSuite) TestNewPackageListFromRefList(c *C) {
	db, _ := sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	coll := NewPackageCollection(db)
	coll.Update(s.p1)
	coll.Update(s.p3)

	s.list.Add(s.p1)
	s.list.Add(s.p3)
	s.list.Add(s.p5)
	s.list.Add(s.p6)

	reflist := NewPackageRefListFromPackageList(s.list)

	_, err := NewPackageListFromRefList(reflist, coll, nil)
	c.Assert(err, ErrorMatches, "unable to load package with key.*")

	coll.Update(s.p5)
	coll.Update(s.p6)

	list, err := NewPackageListFromRefList(reflist, coll, nil)
	c.Assert(err, IsNil)
	c.Check(list.Len(), Equals, 4)
	c.Check(list.Add(s.p4), ErrorMatches, "conflict in package.*")

	list, err = NewPackageListFromRefList(nil, coll, nil)
	c.Assert(err, IsNil)
	c.Check(list.Len(), Equals, 0)
}

func (s *PackageRefListSQLSuite) TestDiff(c *C) {
	db, _ := sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	coll := NewPackageCollection(db)

	packages := []*Package{
		{Name: "lib", Version: "1.0", Architecture: "i386"},      //0
		{Name: "dpkg", Version: "1.7", Architecture: "i386"},     //1
		{Name: "data", Version: "1.1~bp1", Architecture: "all"},  //2
		{Name: "app", Version: "1.1~bp1", Architecture: "i386"},  //3
		{Name: "app", Version: "1.1~bp2", Architecture: "i386"},  //4
		{Name: "app", Version: "1.1~bp2", Architecture: "amd64"}, //5
		{Name: "xyz", Version: "3.0", Architecture: "sparc"},     //6
	}

	for _, p := range packages {
		coll.Update(p)
	}

	listA := NewPackageList()
	listA.Add(packages[0])
	listA.Add(packages[1])
	listA.Add(packages[2])
	listA.Add(packages[3])
	listA.Add(packages[6])

	listB := NewPackageList()
	listB.Add(packages[0])
	listB.Add(packages[2])
	listB.Add(packages[4])
	listB.Add(packages[5])

	reflistA := NewPackageRefListFromPackageList(listA)
	reflistB := NewPackageRefListFromPackageList(listB)

	diffAA, err := reflistA.Diff(reflistA, coll)
	c.Check(err, IsNil)
	c.Check(diffAA, HasLen, 0)

	diffAB, err := reflistA.Diff(reflistB, coll)
	c.Check(err, IsNil)
	c.Check(diffAB, HasLen, 4)

	c.Check(diffAB[0].Left, IsNil)
	c.Check(diffAB[0].Right.String(), Equals, "app_1.1~bp2_amd64")

	c.Check(diffAB[1].Left.String(), Equals, "app_1.1~bp1_i386")
	c.Check(diffAB[1].Right.String(), Equals, "app_1.1~bp2_i386")

	c.Check(diffAB[2].Left.String(), Equals, "dpkg_1.7_i386")
	c.Check(diffAB[2].Right, IsNil)

	c.Check(diffAB[3].Left.String(), Equals, "xyz_3.0_sparc")
	c.Check(diffAB[3].Right, IsNil)

	diffBA, err := reflistB.Diff(reflistA, coll)
	c.Check(err, IsNil)
	c.Check(diffBA, HasLen, 4)

	c.Check(diffBA[0].Right, IsNil)
	c.Check(diffBA[0].Left.String(), Equals, "app_1.1~bp2_amd64")

	c.Check(diffBA[1].Right.String(), Equals, "app_1.1~bp1_i386")
	c.Check(diffBA[1].Left.String(), Equals, "app_1.1~bp2_i386")

	c.Check(diffBA[2].Right.String(), Equals, "dpkg_1.7_i386")
	c.Check(diffBA[2].Left, IsNil)

	c.Check(diffBA[3].Right.String(), Equals, "xyz_3.0_sparc")
	c.Check(diffBA[3].Left, IsNil)

}

func (s *PackageRefListSQLSuite) TestMerge(c *C) {
	db, _ := sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	coll := NewPackageCollection(db)

	packages := []*Package{
		{Name: "lib", Version: "1.0", Architecture: "i386"},                      //0
		{Name: "dpkg", Version: "1.7", Architecture: "i386"},                     //1
		{Name: "data", Version: "1.1~bp1", Architecture: "all"},                  //2
		{Name: "app", Version: "1.1~bp1", Architecture: "i386"},                  //3
		{Name: "app", Version: "1.1~bp2", Architecture: "i386"},                  //4
		{Name: "app", Version: "1.1~bp2", Architecture: "amd64"},                 //5
		{Name: "dpkg", Version: "1.0", Architecture: "i386"},                     //6
		{Name: "xyz", Version: "1.0", Architecture: "sparc"},                     //7
		{Name: "dpkg", Version: "1.0", Architecture: "i386", FilesHash: 0x34445}, //8
		{Name: "app", Version: "1.1~bp2", Architecture: "i386", FilesHash: 0x44}, //9
	}

	for _, p := range packages {
		p.V06Plus = true
		coll.Update(p)
	}

	listA := NewPackageList()
	listA.Add(packages[0])
	listA.Add(packages[1])
	listA.Add(packages[2])
	listA.Add(packages[3])
	listA.Add(packages[7])

	listB := NewPackageList()
	listB.Add(packages[0])
	listB.Add(packages[2])
	listB.Add(packages[4])
	listB.Add(packages[5])
	listB.Add(packages[6])

	listC := NewPackageList()
	listC.Add(packages[0])
	listC.Add(packages[8])
	listC.Add(packages[9])

	reflistA := NewPackageRefListFromPackageList(listA)
	reflistB := NewPackageRefListFromPackageList(listB)
	reflistC := NewPackageRefListFromPackageList(listC)

	mergeAB := reflistA.Merge(reflistB, true, false)
	mergeBA := reflistB.Merge(reflistA, true, false)
	mergeAC := reflistA.Merge(reflistC, true, false)
	mergeBC := reflistB.Merge(reflistC, true, false)
	mergeCB := reflistC.Merge(reflistB, true, false)

	c.Check(toStrSlice(mergeAB), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000000", "Pi386 dpkg 1.0 00000000", "Pi386 lib 1.0 00000000", "Psparc xyz 1.0 00000000"})
	c.Check(toStrSlice(mergeBA), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp1 00000000", "Pi386 dpkg 1.7 00000000", "Pi386 lib 1.0 00000000", "Psparc xyz 1.0 00000000"})
	c.Check(toStrSlice(mergeAC), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pi386 app 1.1~bp2 00000044", "Pi386 dpkg 1.0 00034445", "Pi386 lib 1.0 00000000", "Psparc xyz 1.0 00000000"})
	c.Check(toStrSlice(mergeBC), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000044", "Pi386 dpkg 1.0 00034445", "Pi386 lib 1.0 00000000"})
	c.Check(toStrSlice(mergeCB), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000000", "Pi386 dpkg 1.0 00000000", "Pi386 lib 1.0 00000000"})

	mergeABall := reflistA.Merge(reflistB, false, false)
	mergeBAall := reflistB.Merge(reflistA, false, false)
	mergeACall := reflistA.Merge(reflistC, false, false)
	mergeBCall := reflistB.Merge(reflistC, false, false)
	mergeCBall := reflistC.Merge(reflistB, false, false)

	c.Check(mergeABall, DeepEquals, mergeBAall)
	c.Check(toStrSlice(mergeBAall), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp1 00000000", "Pi386 app 1.1~bp2 00000000",
			"Pi386 dpkg 1.0 00000000", "Pi386 dpkg 1.7 00000000", "Pi386 lib 1.0 00000000", "Psparc xyz 1.0 00000000"})

	c.Check(mergeBCall, Not(DeepEquals), mergeCBall)
	c.Check(toStrSlice(mergeACall), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pi386 app 1.1~bp1 00000000", "Pi386 app 1.1~bp2 00000044", "Pi386 dpkg 1.0 00034445",
			"Pi386 dpkg 1.7 00000000", "Pi386 lib 1.0 00000000", "Psparc xyz 1.0 00000000"})
	c.Check(toStrSlice(mergeBCall), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000044", "Pi386 dpkg 1.0 00034445",
			"Pi386 lib 1.0 00000000"})

	mergeBCwithConflicts := reflistB.Merge(reflistC, false, true)
	c.Check(toStrSlice(mergeBCwithConflicts), DeepEquals,
		[]string{"Pall data 1.1~bp1 00000000", "Pamd64 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000000", "Pi386 app 1.1~bp2 00000044",
			"Pi386 dpkg 1.0 00000000", "Pi386 dpkg 1.0 00034445", "Pi386 lib 1.0 00000000"})
}
