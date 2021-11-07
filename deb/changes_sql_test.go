package deb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aptly-dev/aptly/aptly"
	"github.com/aptly-dev/aptly/console"
	"github.com/aptly-dev/aptly/database"
	"github.com/aptly-dev/aptly/database/sql"
	"github.com/aptly-dev/aptly/files"
	"github.com/aptly-dev/aptly/utils"

	. "gopkg.in/check.v1"
)

type ChangesSQLSuite struct {
	Dir, Path           string
	Reporter            aptly.ResultReporter
	db                  database.Storage
	localRepoCollection *LocalRepoCollection
	packageCollection   *PackageCollection
	packagePool         aptly.PackagePool
	checksumStorage     aptly.ChecksumStorage
	progress            aptly.Progress
	driverName          string
	dataSourceName      string
	tableName           string
}

var _ = Suite(&ChangesSQLSuite{})

func (s *ChangesSQLSuite) SetUpTest(c *C) {
	s.Reporter = &aptly.RecordingResultReporter{
		Warnings:     []string{},
		AddedLines:   []string{},
		RemovedLines: []string{},
	}
	s.Dir = c.MkDir()
	s.Path = filepath.Join(s.Dir, "calamares.changes")
	err := utils.CopyFile("testdata/changes/calamares.changes", s.Path)
	c.Assert(err, IsNil)

	s.driverName = "sqlite3"
	s.dataSourceName = fmt.Sprintf("file:%s/sql_test.db", c.MkDir())
	s.tableName = "testtable"
	s.db, _ = sql.NewOpenDB(s.driverName, s.dataSourceName, s.tableName)
	s.localRepoCollection = NewLocalRepoCollection(s.db)
	s.packageCollection = NewPackageCollection(s.db)

	s.checksumStorage = files.NewMockChecksumStorage()
	s.packagePool = files.NewPackagePool(s.Dir, false)
	s.progress = console.NewProgress()
	s.progress.Start()
}

func (s *ChangesSQLSuite) TearDownTest(c *C) {
	s.progress.Shutdown()
	s.db.Close()
}

func (s *ChangesSQLSuite) TestParseAndVerify(c *C) {
	changes, err := NewChanges(s.Path)
	c.Assert(err, IsNil)

	err = changes.VerifyAndParse(true, true, &NullVerifier{})
	c.Check(err, IsNil)

	c.Check(changes.Distribution, Equals, "sid")
	c.Check(changes.Files, HasLen, 4)
	c.Check(changes.Files[0].Filename, Equals, "calamares_0+git20141127.99.dsc")
	c.Check(changes.Files[0].Checksums.Size, Equals, int64(1106))
	c.Check(changes.Files[0].Checksums.MD5, Equals, "05fd8f3ffe8f362c5ef9bad2f936a56e")
	c.Check(changes.Files[0].Checksums.SHA1, Equals, "79f10e955dab6eb25b7f7bae18213f367a3a0396")
	c.Check(changes.Files[0].Checksums.SHA256, Equals, "35b3280a7b1ffe159a276128cb5c408d687318f60ecbb8ab6dedb2e49c4e82dc")
	c.Check(changes.BasePath, Equals, s.Dir)
	c.Check(changes.Architectures, DeepEquals, []string{"source", "amd64"})
	c.Check(changes.Source, Equals, "calamares")
	c.Check(changes.Binary, DeepEquals, []string{"calamares", "calamares-dbg"})
}

func (s *ChangesSQLSuite) TestCollectChangesFiles(c *C) {
	changesFiles, failedFiles := CollectChangesFiles([]string{"testdata/changes"}, s.Reporter)

	c.Check(failedFiles, HasLen, 0)
	c.Check(changesFiles, DeepEquals, []string{
		"testdata/changes/calamares.changes",
		"testdata/changes/hardlink_0.2.1-invalidfiles_amd64.changes",
		"testdata/changes/hardlink_0.2.1-invalidsig_amd64.changes",
		"testdata/changes/hardlink_0.2.1_amd64.changes",
	})
}

func (s *ChangesSQLSuite) TestImportChangesFiles(c *C) {
	repo := NewLocalRepo("test", "Test Comment")
	c.Assert(s.localRepoCollection.Add(repo), IsNil)

	origFailedFiles := []string{
		"testdata/changes/calamares.changes",
		"testdata/changes/hardlink_0.2.1-invalidfiles_amd64.changes",
		"testdata/changes/hardlink_0.2.1-invalidsig_amd64.changes",
		"testdata/changes/hardlink_0.2.0_i386.deb",
	}
	origProcessedFiles := []string{
		"testdata/changes/hardlink_0.2.1.dsc",
		"testdata/changes/hardlink_0.2.1.tar.gz",
		"testdata/changes/hardlink_0.2.1_amd64.deb",
		"testdata/changes/hardlink_0.2.1_amd64.buildinfo",
		"testdata/changes/hardlink_0.2.1_amd64.changes",
	}

	var expectedProcessedFiles, expectedFailedFiles []string

	for _, path := range origFailedFiles {
		filename := filepath.Join(s.Dir, filepath.Base(path))
		utils.CopyFile(path, filename)
		expectedFailedFiles = append(expectedFailedFiles, filename)
	}

	for _, path := range origProcessedFiles {
		filename := filepath.Join(s.Dir, filepath.Base(path))
		utils.CopyFile(path, filename)
		expectedProcessedFiles = append(expectedProcessedFiles, filename)
	}

	changesFiles, failedFiles := CollectChangesFiles([]string{s.Dir}, s.Reporter)
	c.Check(failedFiles, HasLen, 0)

	processedFiles, failedFiles, err := ImportChangesFiles(
		append(changesFiles, "testdata/changes/notexistent.changes"),
		s.Reporter, true, true, false, false, &NullVerifier{},
		"test", s.progress, s.localRepoCollection, s.packageCollection, s.packagePool, func(database.ReaderWriter) aptly.ChecksumStorage { return s.checksumStorage },
		nil, nil)
	c.Assert(err, IsNil)
	c.Check(failedFiles, DeepEquals, append(expectedFailedFiles, "testdata/changes/notexistent.changes"))
	c.Check(processedFiles, DeepEquals, expectedProcessedFiles)
}

func (s *ChangesSQLSuite) TestPrepare(c *C) {
	changes, err := NewChanges("testdata/changes/hardlink_0.2.1_amd64.changes")
	c.Assert(err, IsNil)
	err = changes.Prepare()
	c.Assert(err, IsNil)

	_, err = os.Stat(filepath.Join(changes.TempDir, "hardlink_0.2.1_amd64.changes"))
	c.Check(err, IsNil)
}

func (s *ChangesSQLSuite) TestPackageQuery(c *C) {
	changes, err := NewChanges(s.Path)
	c.Assert(err, IsNil)

	err = changes.VerifyAndParse(true, true, &NullVerifier{})
	c.Check(err, IsNil)

	q := changes.PackageQuery()
	c.Check(q.String(), Equals,
		"(($Architecture (= amd64)) | (($Architecture (= source)) | ($Architecture (= )))), ((($PackageType (= source)), (Name (= calamares))) | ((!($PackageType (= source))), (((Name (= calamares-dbg)) | (Name (= calamares))) | ((Source (= calamares)), ((Name (= calamares-dbg-dbgsym)) | (Name (= calamares-dbgsym)))))))")
}
