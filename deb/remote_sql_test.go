package deb

import (
	"errors"
	"fmt"
	"sort"

	"github.com/aptly-dev/aptly/aptly"
	"github.com/aptly-dev/aptly/console"
	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"
	"github.com/aptly-dev/aptly/files"
	"github.com/aptly-dev/aptly/http"
	"github.com/aptly-dev/aptly/utils"

	. "gopkg.in/check.v1"
)

type RemoteRepoSQLSuite struct {
	PackageListMixinSuite
	repo              *RemoteRepo
	flat              *RemoteRepo
	downloader        *http.FakeDownloader
	progress          aptly.Progress
	db                database.Storage
	collectionFactory *CollectionFactory
	packagePool       aptly.PackagePool
	cs                aptly.ChecksumStorage
}

var _ = Suite(&RemoteRepoSQLSuite{})

func (s *RemoteRepoSQLSuite) SetUpTest(c *C) {
	s.repo, _ = NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian", "squeeze", []string{"main"}, []string{}, false, false, false)
	s.flat, _ = NewRemoteRepo("exp42", "http://repos.express42.com/virool/precise/", "./", []string{}, []string{}, false, false, false)
	s.downloader = http.NewFakeDownloader().ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)
	s.progress = console.NewProgress()
	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)
	s.collectionFactory = NewCollectionFactory(s.db)
	s.packagePool = files.NewPackagePool(c.MkDir(), false)
	s.cs = files.NewMockChecksumStorage()
	s.SetUpPackages()
	s.progress.Start()
}

func (s *RemoteRepoSQLSuite) TearDownTest(c *C) {
	s.progress.Shutdown()
	s.db.Close()
}

func (s *RemoteRepoSQLSuite) TestInvalidURL(c *C) {
	_, err := NewRemoteRepo("s", "http://lolo%2", "squeeze", []string{"main"}, []string{}, false, false, false)
	c.Assert(err, ErrorMatches, ".*(hexadecimal escape in host|percent-encoded characters in host|invalid URL escape).*")
}

func (s *RemoteRepoSQLSuite) TestFlatCreation(c *C) {
	c.Check(s.flat.IsFlat(), Equals, true)
	c.Check(s.flat.Distribution, Equals, "./")
	c.Check(s.flat.Architectures, IsNil)
	c.Check(s.flat.Components, IsNil)

	flat2, _ := NewRemoteRepo("flat2", "http://pkg.jenkins-ci.org/debian-stable", "binary/", []string{}, []string{}, false, false, false)
	c.Check(flat2.IsFlat(), Equals, true)
	c.Check(flat2.Distribution, Equals, "./binary/")

	_, err := NewRemoteRepo("fl", "http://some.repo/", "./", []string{"main"}, []string{}, false, false, false)
	c.Check(err, ErrorMatches, "components aren't supported for flat repos")
}

func (s *RemoteRepoSQLSuite) TestString(c *C) {
	c.Check(s.repo.String(), Equals, "[yandex]: http://mirror.yandex.ru/debian/ squeeze")
	c.Check(s.flat.String(), Equals, "[exp42]: http://repos.express42.com/virool/precise/ ./")

	s.repo.DownloadSources = true
	s.repo.DownloadUdebs = true
	s.repo.DownloadInstaller = true
	s.flat.DownloadSources = true
	c.Check(s.repo.String(), Equals, "[yandex]: http://mirror.yandex.ru/debian/ squeeze [src] [udeb] [installer]")
	c.Check(s.flat.String(), Equals, "[exp42]: http://repos.express42.com/virool/precise/ ./ [src]")
}

func (s *RemoteRepoSQLSuite) TestNumPackages(c *C) {
	c.Check(s.repo.NumPackages(), Equals, 0)
	s.repo.packageRefs = s.reflist
	c.Check(s.repo.NumPackages(), Equals, 3)
}

func (s *RemoteRepoSQLSuite) TestIsFlat(c *C) {
	c.Check(s.repo.IsFlat(), Equals, false)
	c.Check(s.flat.IsFlat(), Equals, true)
}

func (s *RemoteRepoSQLSuite) TestRefList(c *C) {
	s.repo.packageRefs = s.reflist
	c.Check(s.repo.RefList(), Equals, s.reflist)
}

func (s *RemoteRepoSQLSuite) TestReleaseURL(c *C) {
	c.Assert(s.repo.ReleaseURL("Release").String(), Equals, "http://mirror.yandex.ru/debian/dists/squeeze/Release")
	c.Assert(s.repo.ReleaseURL("InRelease").String(), Equals, "http://mirror.yandex.ru/debian/dists/squeeze/InRelease")

	c.Assert(s.flat.ReleaseURL("Release").String(), Equals, "http://repos.express42.com/virool/precise/Release")
}

func (s *RemoteRepoSQLSuite) TestIndexesRootURL(c *C) {
	c.Assert(s.repo.IndexesRootURL().String(), Equals, "http://mirror.yandex.ru/debian/dists/squeeze/")

	c.Assert(s.flat.IndexesRootURL().String(), Equals, "http://repos.express42.com/virool/precise/")
}

func (s *RemoteRepoSQLSuite) TestBinaryPath(c *C) {
	c.Assert(s.repo.BinaryPath("main", "amd64"), Equals, "main/binary-amd64/Packages")
}

func (s *RemoteRepoSQLSuite) TestUdebPath(c *C) {
	c.Assert(s.repo.UdebPath("main", "amd64"), Equals, "main/debian-installer/binary-amd64/Packages")
}

func (s *RemoteRepoSQLSuite) TestSourcesPath(c *C) {
	c.Assert(s.repo.SourcesPath("main"), Equals, "main/source/Sources")
}

func (s *RemoteRepoSQLSuite) TestInstallerPath(c *C) {
	c.Assert(s.repo.InstallerPath("main", "amd64"), Equals, "main/installer-amd64/current/images/SHA256SUMS")
}

func (s *RemoteRepoSQLSuite) TestFlatBinaryPath(c *C) {
	c.Assert(s.flat.FlatBinaryPath(), Equals, "Packages")
}

func (s *RemoteRepoSQLSuite) TestFlatSourcesPath(c *C) {
	c.Assert(s.flat.FlatSourcesPath(), Equals, "Sources")
}

func (s *RemoteRepoSQLSuite) TestPackageURL(c *C) {
	c.Assert(s.repo.PackageURL("pool/main/0/0ad/0ad_0~r11863-2_i386.deb").String(), Equals,
		"http://mirror.yandex.ru/debian/pool/main/0/0ad/0ad_0~r11863-2_i386.deb")
}

func (s *RemoteRepoSQLSuite) TestFetch(c *C) {
	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)
	c.Assert(s.repo.Architectures, DeepEquals, []string{"amd64", "armel", "armhf", "i386", "powerpc"})
	c.Assert(s.repo.Components, DeepEquals, []string{"main"})
	c.Assert(s.downloader.Empty(), Equals, true)

	c.Check(s.repo.ReleaseFiles, HasLen, 39)
	c.Check(s.repo.ReleaseFiles["main/binary-i386/Packages.bz2"], DeepEquals,
		utils.ChecksumInfo{
			Size:   734,
			MD5:    "7954ed80936429687122b554620c1b5b",
			SHA1:   "95a463a0739bf9ff622c8d68f6e4598d400f5248",
			SHA256: "377890a26f99db55e117dfc691972dcbbb7d8be1630c8fc8297530c205377f2b"})
}

func (s *RemoteRepoSQLSuite) TestFetchNullVerifier1(c *C) {
	downloader := http.NewFakeDownloader()
	downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/InRelease", &http.Error{Code: 404})
	downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)
	downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release.gpg", "GPG")

	err := s.repo.Fetch(downloader, &NullVerifier{})
	c.Assert(err, IsNil)
	c.Assert(s.repo.Architectures, DeepEquals, []string{"amd64", "armel", "armhf", "i386", "powerpc"})
	c.Assert(s.repo.Components, DeepEquals, []string{"main"})
	c.Assert(downloader.Empty(), Equals, true)
}

func (s *RemoteRepoSQLSuite) TestFetchNullVerifier2(c *C) {
	downloader := http.NewFakeDownloader()
	downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/InRelease", exampleReleaseFile)

	err := s.repo.Fetch(downloader, &NullVerifier{})
	c.Assert(err, IsNil)
	c.Assert(s.repo.Architectures, DeepEquals, []string{"amd64", "armel", "armhf", "i386", "powerpc"})
	c.Assert(s.repo.Components, DeepEquals, []string{"main"})
	c.Assert(downloader.Empty(), Equals, true)
}

func (s *RemoteRepoSQLSuite) TestFetchWrongArchitecture(c *C) {
	s.repo, _ = NewRemoteRepo("s", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{"xyz"}, false, false, false)
	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, ErrorMatches, "architecture xyz not available in repo.*")
}

func (s *RemoteRepoSQLSuite) TestFetchWrongComponent(c *C) {
	s.repo, _ = NewRemoteRepo("s", "http://mirror.yandex.ru/debian/", "squeeze", []string{"xyz"}, []string{"i386"}, false, false, false)
	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, ErrorMatches, "component xyz not available in repo.*")
}

func (s *RemoteRepoSQLSuite) TestEncodeDecode(c *C) {
	repo := &RemoteRepo{}
	err := repo.Decode(s.repo.Encode())
	c.Assert(err, IsNil)

	c.Check(repo.Name, Equals, "yandex")
	c.Check(repo.ArchiveRoot, Equals, "http://mirror.yandex.ru/debian/")
}

func (s *RemoteRepoSQLSuite) TestKey(c *C) {
	c.Assert(len(s.repo.Key()), Equals, 37)
	c.Assert(s.repo.Key()[0], Equals, byte('R'))
}

func (s *RemoteRepoSQLSuite) TestRefKey(c *C) {
	c.Assert(len(s.repo.RefKey()), Equals, 37)
	c.Assert(s.repo.RefKey()[0], Equals, byte('E'))
	c.Assert(s.repo.RefKey()[1:], DeepEquals, s.repo.Key()[1:])
}

func (s *RemoteRepoSQLSuite) TestDownload(c *C) {
	s.repo.Architectures = []string{"i386"}

	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err := s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(3))
	c.Check(queue, HasLen, 1)
	c.Check(queue[0].File.DownloadURL(), Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)

	pkg, err := s.collectionFactory.PackageCollection().ByKey(s.repo.packageRefs.Refs[0])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "amanda-client")

	// Next call must return an empty download list with option "skip-existing-packages"
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)
	err = s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err = s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, true)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(0))
	c.Check(queue, HasLen, 0)

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)

	// Next call must return the download list without option "skip-existing-packages"
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)
	err = s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err = s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(3))
	c.Check(queue, HasLen, 1)
	c.Check(queue[0].File.DownloadURL(), Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)
}

func (s *RemoteRepoSQLSuite) TestDownloadWithInstaller(c *C) {
	s.repo.Architectures = []string{"i386"}
	s.repo.DownloadInstaller = true

	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/installer-i386/current/images/SHA256SUMS", exampleInstallerHashSumFile)
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/installer-i386/current/images/MANIFEST", exampleInstallerManifestFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err := s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(3)+int64(len(exampleInstallerManifestFile)))
	c.Check(queue, HasLen, 2)

	q := make([]string, 2)
	for i := range q {
		q[i] = queue[i].File.DownloadURL()
	}
	sort.Strings(q)
	c.Check(q[0], Equals, "dists/squeeze/main/installer-i386/current/images/MANIFEST")
	c.Check(q[1], Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)

	pkg, err := s.collectionFactory.PackageCollection().ByKey(s.repo.packageRefs.Refs[0])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "amanda-client")

	pkg, err = s.collectionFactory.PackageCollection().ByKey(s.repo.packageRefs.Refs[1])
	c.Assert(err, IsNil)
	c.Check(pkg.Name, Equals, "installer")
}

func (s *RemoteRepoSQLSuite) TestDownloadWithSources(c *C) {
	s.repo.Architectures = []string{"i386"}
	s.repo.DownloadSources = true

	err := s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources", exampleSourcesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err := s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(15))
	c.Check(queue, HasLen, 4)

	q := make([]string, 4)
	for i := range q {
		q[i] = queue[i].File.DownloadURL()
	}
	sort.Strings(q)
	c.Check(q[3], Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")
	c.Check(q[1], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0-4.dsc")
	c.Check(q[2], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0.orig.tar.gz")
	c.Check(q[0], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0-4.debian.tar.gz")

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)

	pkg, err := s.collectionFactory.PackageCollection().ByKey(s.repo.packageRefs.Refs[0])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "amanda-client")

	pkg, err = s.collectionFactory.PackageCollection().ByKey(s.repo.packageRefs.Refs[1])
	c.Assert(err, IsNil)
	c.Check(pkg.Name, Equals, "access-modifier-checker")

	// Next call must return an empty download list with option "skip-existing-packages"
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)

	err = s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources", exampleSourcesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err = s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, true)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(0))
	c.Check(queue, HasLen, 0)

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)

	// Next call must return the download list without option "skip-existing-packages"
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/Release", exampleReleaseFile)

	err = s.repo.Fetch(s.downloader, nil)
	c.Assert(err, IsNil)

	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/binary-i386/Packages", examplePackagesFile)
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.bz2", &http.Error{Code: 404})
	s.downloader.ExpectError("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources.gz", &http.Error{Code: 404})
	s.downloader.ExpectResponse("http://mirror.yandex.ru/debian/dists/squeeze/main/source/Sources", exampleSourcesFile)

	err = s.repo.DownloadPackageIndexes(s.progress, s.downloader, nil, s.collectionFactory, false)
	c.Assert(err, IsNil)
	c.Assert(s.downloader.Empty(), Equals, true)

	queue, size, err = s.repo.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(15))
	c.Check(queue, HasLen, 4)

	s.repo.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.repo.packageRefs, NotNil)
}

func (s *RemoteRepoSQLSuite) TestDownloadFlat(c *C) {
	downloader := http.NewFakeDownloader()
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)

	err := s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err := s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(3))
	c.Check(queue, HasLen, 1)
	c.Check(queue[0].File.DownloadURL(), Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)

	pkg, err := s.collectionFactory.PackageCollection().ByKey(s.flat.packageRefs.Refs[0])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "amanda-client")

	// Next call must return an empty download list with option "skip-existing-packages"
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)

	err = s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err = s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, true)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(0))
	c.Check(queue, HasLen, 0)

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)

	// Next call must return the download list without option "skip-existing-packages"
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)

	err = s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err = s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(3))
	c.Check(queue, HasLen, 1)
	c.Check(queue[0].File.DownloadURL(), Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)
}

func (s *RemoteRepoSQLSuite) TestDownloadWithSourcesFlat(c *C) {
	s.flat.DownloadSources = true

	downloader := http.NewFakeDownloader()
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Sources", exampleSourcesFile)

	err := s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err := s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(15))
	c.Check(queue, HasLen, 4)

	q := make([]string, 4)
	for i := range q {
		q[i] = queue[i].File.DownloadURL()
	}
	sort.Strings(q)
	c.Check(q[3], Equals, "pool/main/a/amanda/amanda-client_3.3.1-3~bpo60+1_amd64.deb")
	c.Check(q[1], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0-4.dsc")
	c.Check(q[2], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0.orig.tar.gz")
	c.Check(q[0], Equals, "pool/main/a/access-modifier-checker/access-modifier-checker_1.0-4.debian.tar.gz")

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)

	pkg, err := s.collectionFactory.PackageCollection().ByKey(s.flat.packageRefs.Refs[0])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "amanda-client")

	pkg, err = s.collectionFactory.PackageCollection().ByKey(s.flat.packageRefs.Refs[1])
	c.Assert(err, IsNil)

	c.Check(pkg.Name, Equals, "access-modifier-checker")

	// Next call must return an empty download list with option "skip-existing-packages"
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Sources", exampleSourcesFile)

	err = s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err = s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, true)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(0))
	c.Check(queue, HasLen, 0)

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)

	// Next call must return the download list without option "skip-existing-packages"
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Release", exampleReleaseFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Packages.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Packages", examplePackagesFile)
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.bz2", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.gz", &http.Error{Code: 404})
	downloader.ExpectError("http://repos.express42.com/virool/precise/Sources.xz", &http.Error{Code: 404})
	downloader.ExpectResponse("http://repos.express42.com/virool/precise/Sources", exampleSourcesFile)

	err = s.flat.Fetch(downloader, nil)
	c.Assert(err, IsNil)

	err = s.flat.DownloadPackageIndexes(s.progress, downloader, nil, s.collectionFactory, true)
	c.Assert(err, IsNil)
	c.Assert(downloader.Empty(), Equals, true)

	queue, size, err = s.flat.BuildDownloadQueue(s.packagePool, s.collectionFactory.PackageCollection(), s.cs, false)
	c.Assert(err, IsNil)
	c.Check(size, Equals, int64(15))
	c.Check(queue, HasLen, 4)

	s.flat.FinalizeDownload(s.collectionFactory, nil)
	c.Assert(s.flat.packageRefs, NotNil)
}

type RemoteRepoCollectionSQLSuite struct {
	PackageListMixinSuite
	db         database.Storage
	collection *RemoteRepoCollection
}

var _ = Suite(&RemoteRepoCollectionSQLSuite{})

func (s *RemoteRepoCollectionSQLSuite) SetUpTest(c *C) {
	driverName := "sqlite3"
	dataSourceName := fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	tableName := "testtable"
	s.db, _ = sql.NewOpenDB(driverName, dataSourceName, tableName)
	s.collection = NewRemoteRepoCollection(s.db)
	s.SetUpPackages()
}

func (s *RemoteRepoCollectionSQLSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *RemoteRepoCollectionSQLSuite) TestAddByName(c *C) {
	_, err := s.collection.ByName("yandex")
	c.Assert(err, ErrorMatches, "*.not found")

	repo, _ := NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{}, false, false, false)
	c.Assert(s.collection.Add(repo), IsNil)
	c.Assert(s.collection.Add(repo), ErrorMatches, ".*already exists")

	r, err := s.collection.ByName("yandex")
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())

	collection := NewRemoteRepoCollection(s.db)
	r, err = collection.ByName("yandex")
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())
}

func (s *RemoteRepoCollectionSQLSuite) TestByUUID(c *C) {
	_, err := s.collection.ByUUID("some-uuid")
	c.Assert(err, ErrorMatches, "*.not found")

	repo, _ := NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{}, false, false, false)
	c.Assert(s.collection.Add(repo), IsNil)

	r, err := s.collection.ByUUID(repo.UUID)
	c.Assert(err, IsNil)
	c.Assert(r, Equals, repo)

	collection := NewRemoteRepoCollection(s.db)
	r, err = collection.ByUUID(repo.UUID)
	c.Assert(err, IsNil)
	c.Assert(r.String(), Equals, repo.String())
}

func (s *RemoteRepoCollectionSQLSuite) TestUpdateLoadComplete(c *C) {
	repo, _ := NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{}, false, false, false)
	c.Assert(s.collection.Update(repo), IsNil)

	collection := NewRemoteRepoCollection(s.db)
	r, err := collection.ByName("yandex")
	c.Assert(err, IsNil)
	c.Assert(r.packageRefs, IsNil)

	repo.packageRefs = s.reflist
	c.Assert(s.collection.Update(repo), IsNil)

	collection = NewRemoteRepoCollection(s.db)
	r, err = collection.ByName("yandex")
	c.Assert(err, IsNil)
	c.Assert(r.packageRefs, IsNil)
	c.Assert(r.NumPackages(), Equals, 0)
	c.Assert(s.collection.LoadComplete(r), IsNil)
	c.Assert(r.NumPackages(), Equals, 3)
}

func (s *RemoteRepoCollectionSQLSuite) TestForEachAndLen(c *C) {
	repo, _ := NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{}, false, false, false)
	s.collection.Add(repo)

	count := 0
	err := s.collection.ForEach(func(*RemoteRepo) error {
		count++
		return nil
	})
	c.Assert(count, Equals, 1)
	c.Assert(err, IsNil)

	c.Check(s.collection.Len(), Equals, 1)

	e := errors.New("c")

	err = s.collection.ForEach(func(*RemoteRepo) error {
		return e
	})
	c.Assert(err, Equals, e)
}

func (s *RemoteRepoCollectionSQLSuite) TestDrop(c *C) {
	repo1, _ := NewRemoteRepo("yandex", "http://mirror.yandex.ru/debian/", "squeeze", []string{"main"}, []string{}, false, false, false)
	s.collection.Add(repo1)

	repo2, _ := NewRemoteRepo("tyndex", "http://mirror.yandex.ru/debian/", "wheezy", []string{"main"}, []string{}, false, false, false)
	s.collection.Add(repo2)

	r1, _ := s.collection.ByUUID(repo1.UUID)
	c.Check(r1, Equals, repo1)

	err := s.collection.Drop(repo1)
	c.Check(err, IsNil)

	_, err = s.collection.ByUUID(repo1.UUID)
	c.Check(err, ErrorMatches, "mirror .* not found")

	collection := NewRemoteRepoCollection(s.db)
	_, err = collection.ByName("yandex")
	c.Check(err, ErrorMatches, "mirror .* not found")

	r2, _ := collection.ByName("tyndex")
	c.Check(r2.String(), Equals, repo2.String())

	c.Check(s.collection.Drop(repo1), ErrorMatches, "repo not found")
}
