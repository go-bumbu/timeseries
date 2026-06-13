package testdbs

import (
	"errors"
	"flag"
	"fmt"
	sqliteNoCgo "github.com/glebarez/sqlite"
	"github.com/hashicorp/go-multierror"
	sqlitecgo "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"strings"
)

// return the path for a db name
func dbPath(name, suffix, tmpdir string, isLocal bool) string {
	if name == "" {
		name = defaultDbName
	}
	dbName := fmt.Sprintf("%s.%s.sqlite", name, suffix)
	if isLocal {
		tmpdir = "./" + testDbDir + "_sqlite"
		if err := os.MkdirAll(tmpdir, 0750); err != nil {
			panic(fmt.Sprintf("error creating local temporary directory: %v", err))
		}
	}
	return fmt.Sprintf("%s/%s", tmpdir, dbName)
}

const testDbDir = "testdbs"

func mkTmpDir() string {
	dir, err := os.MkdirTemp("", testDbDir+"_sqlite")
	if err != nil {
		panic(fmt.Sprintf("error creating temporary directory: %v", err))
	}
	return dir
}

// Add a flag to run sqlite on the local dir instead of on the tmpdir
var sqliteLoca *bool

func init() {
	sqliteLoca = flag.Bool("localsqlite", false, "create sqlite DBs in the CWD")
}

func sqliteLocal() bool {
	if sqliteLoca == nil {
		panic("testing: sqliteLocal called before Init")
	}
	// Catch code that calls this from TestMain without first calling flag.Parse.
	if !flag.Parsed() {
		panic("testing: sqliteLocal called before Parse")
	}
	return *sqliteLoca
}

// ===============================================================================
// Sqlite without CGO
// ===============================================================================

const (
	noCgoSqliteSuffix = "no_CGO"
	DBTypeSqliteNOCgo = "SqliteNoCgo"
)

type SqliteNoCgo struct {
	dir     string
	isLocal bool
	logger  logger.Interface
	pool    map[string]*gorm.DB
}

func (c *SqliteNoCgo) DbType() string {
	return DBTypeSqliteNOCgo
}
func (c *SqliteNoCgo) Init(logger logger.Interface) {
	c.logger = logger
	c.pool = map[string]*gorm.DB{}

	_, localSqliteEnv := os.LookupEnv(LocalSqliteEnv)
	if localSqliteEnv || sqliteLocal() {
		c.isLocal = true
		c.dir = "./"
	} else {
		c.dir = mkTmpDir()
		c.isLocal = false
	}
}
func (c *SqliteNoCgo) Conn() *gorm.DB {
	return c.ConnDbName(defaultDbName)
}

func (c *SqliteNoCgo) ConnDbName(name string) *gorm.DB {
	name = normalizeDbName(name)
	dbConn, exists := c.pool[name]
	if exists {
		return dbConn
	}
	dbFile := dbPath(name, noCgoSqliteSuffix, c.dir, c.isLocal)
	if _, err := os.Stat(dbFile); err == nil {
		err = os.RemoveAll(dbFile)
		if err != nil {
			panic(fmt.Sprintf("error while removing dbfile: %v", err))
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		panic(fmt.Sprintf(" while doing stat on dbfile: %v", err))
	}

	db, err := gorm.Open(sqliteNoCgo.Open(dbFile), &gorm.Config{
		Logger: c.logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to open test database: %v", err))
	}
	c.pool[name] = db
	return db
}

func (c *SqliteNoCgo) Close(name string) error {

	dbConn, exists := c.pool[name]
	if !exists {
		return fmt.Errorf("db connection with name %s not found", name)
	}

	db, err := dbConn.DB()
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}

	if !c.isLocal {
		if !strings.Contains(c.dir, testDbDir) {
			panic("refusing to delete the dir since it does not seem to be from testdbs")
		}
		err = os.RemoveAll(c.dir)
		if err != nil {
			return fmt.Errorf("error cleaning up temporary directory: %w", err)
		}
	}
	return nil
}

func (c *SqliteNoCgo) CloseAll() error {
	var merr error
	for name := range c.pool {
		err := c.Close(name)
		if err != nil {
			merr = multierror.Append(merr, err)
		}

	}
	return merr
}

// ===============================================================================
// Sqlite using CGO
// ===============================================================================

const (
	CgoSqliteSuffix = "with_CGO"
	DBTypeSqliteCgo = "SqliteWithCgo"
)

type SqliteCgo struct {
	dir     string
	isLocal bool
	logger  logger.Interface
	pool    map[string]*gorm.DB
}

func (c *SqliteCgo) DbType() string {
	return DBTypeSqliteCgo
}
func (c *SqliteCgo) Init(logger logger.Interface) {
	c.logger = logger
	c.pool = map[string]*gorm.DB{}

	_, localSqliteEnv := os.LookupEnv(LocalSqliteEnv)
	if localSqliteEnv || sqliteLocal() {
		c.isLocal = true
		c.dir = "./"
	} else {
		c.dir = mkTmpDir()
		c.isLocal = false
	}
}
func (c *SqliteCgo) Conn() *gorm.DB {
	return c.ConnDbName(defaultDbName)
}

func (c *SqliteCgo) ConnDbName(name string) *gorm.DB {
	name = normalizeDbName(name)
	dbConn, exists := c.pool[name]
	if exists {
		return dbConn
	}
	dbFile := dbPath(name, CgoSqliteSuffix, c.dir, c.isLocal)
	if _, err := os.Stat(dbFile); err == nil {
		err = os.RemoveAll(dbFile)
		if err != nil {
			panic(fmt.Sprintf("error while removing dbfile: %v", err))
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		panic(fmt.Sprintf(" while doing stat on dbfile: %v", err))
	}

	db, err := gorm.Open(sqlitecgo.Open(dbFile), &gorm.Config{
		Logger: c.logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to open test database: %v", err))
	}
	c.pool[name] = db
	return db
}

func (c *SqliteCgo) Close(name string) error {

	dbConn, exists := c.pool[name]
	if !exists {
		return fmt.Errorf("db connection with name %s not found", name)
	}

	db, err := dbConn.DB()
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}

	if !c.isLocal {
		if !strings.Contains(c.dir, testDbDir) {
			panic("refusing to delete the dir since it does not seem to be from testdbs")
		}
		err = os.RemoveAll(c.dir)
		if err != nil {
			return fmt.Errorf("error cleaning up temporary directory: %w", err)
		}
	}
	return nil
}

func (c *SqliteCgo) CloseAll() error {
	var merr error
	for name := range c.pool {
		err := c.Close(name)
		if err != nil {
			merr = multierror.Append(merr, err)
		}

	}
	return merr
}
