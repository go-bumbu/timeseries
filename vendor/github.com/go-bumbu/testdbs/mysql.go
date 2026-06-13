package testdbs

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync"
	"time"
)

const (
	DBTypeMysql = "mysql"
)

type testDBMysql struct {
	once   sync.Once
	logger logger.Interface
	host   string
	port   string
	pool   map[string]*gorm.DB
	clean  func()
}

func (c *testDBMysql) Close(name string) error {
	db, exists := c.pool[name]
	if !exists {
		return fmt.Errorf("db connection with name %s not found", name)
	}
	under, err := db.DB()
	if err != nil {
		return fmt.Errorf("unable to get underlying DB: %w", err)
	}
	return under.Close()
}

func (c *testDBMysql) CloseAll() error {
	defer c.clean()
	var merr error
	for name := range c.pool {
		err := c.Close(name)
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}
	return merr
}

func (c *testDBMysql) DbType() string {
	return DBTypeMysql
}

const (
	mysqlPassword = "password"
	mysqlUser     = "testuser"
)

func (c *testDBMysql) Init(logger logger.Interface) {
	c.logger = logger
	c.pool = map[string]*gorm.DB{}

	c.once.Do(func() {
		ctx := context.Background()

		req := testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": mysqlPassword,
				"MYSQL_DATABASE":      defaultDbName,
				"MYSQL_USER":          mysqlUser,
				"MYSQL_PASSWORD":      mysqlPassword,
			},
			// mysql:8.0 opens port 3306 for a temporary init server, then
			// restarts the real one. Waiting only for the port races into that
			// init window (intermittent "unexpected EOF" / "invalid connection").
			// "ready for connections" is logged twice: wait for the second.
			WaitingFor: wait.ForAll(
				wait.ForLog(".*ready for connections.*").AsRegexp().WithOccurrence(2).WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("3306/tcp").WithStartupTimeout(60*time.Second),
			),
		}
		mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to start MySQL container: %v", err))
		}

		host, err := mysqlContainer.Host(ctx)
		if err != nil {
			panic(fmt.Sprintf("failed to get MySQL container host: %v", err))
		}
		c.host = host

		port, err := mysqlContainer.MappedPort(ctx, "3306")
		if err != nil {
			panic(fmt.Sprintf("failed to get MySQL container port: %v", err))
		}
		c.port = port.Port()

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", mysqlUser, mysqlPassword, host, port.Port(), defaultDbName)
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to connect to MySQL test database: %v", err))
		}
		cleanFn := func() {
			if err := mysqlContainer.Terminate(ctx); err != nil {
				panic(fmt.Sprintf("failed to terminate MySQL container: %v", err))
			}
		}
		c.clean = cleanFn
		c.pool[normalizeDbName(defaultDbName)] = db
	})
}

func (c *testDBMysql) Conn() *gorm.DB {
	return c.ConnDbName(defaultDbName)
}

func (c *testDBMysql) ConnDbName(name string) *gorm.DB {
	name = normalizeDbName(name)
	dbConn, exists := c.pool[name]
	if exists {
		return dbConn
	}

	dsn := fmt.Sprintf("root:%s@tcp(%s:%s)/", mysqlPassword, c.host, c.port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + name)
	if err != nil {
		panic(err)
	}
	grantQuery := fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'", name, mysqlUser)
	_, err = db.Exec(grantQuery)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		panic(err)
	}

	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", mysqlUser, mysqlPassword, c.host, c.port, name)
	gormDb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: c.logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to MySQL test database: %v", err))
	}

	c.pool[name] = gormDb
	return gormDb

}
