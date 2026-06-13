package testdbs

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync"
	"time"
)

const (
	DBTypePostgres = "postgres"
)

type testDBPostgres struct {
	once   sync.Once
	logger logger.Interface
	host   string
	port   string
	pool   map[string]*gorm.DB
	clean  func()
}

func (c *testDBPostgres) Close(name string) error {
	db, exists := c.pool[name]
	if !exists {
		return fmt.Errorf("db connection with name %s not found", name)
	}
	underlyingDb, err := db.DB()
	if err != nil {
		return fmt.Errorf("unable to get underlying DB: %w", err)
	}

	// Set the max connections to zero before closing
	underlyingDb.SetMaxOpenConns(1)
	underlyingDb.SetMaxIdleConns(0)
	underlyingDb.SetConnMaxLifetime(time.Microsecond)

	// Wait a little to allow existing connections to close
	time.Sleep(100 * time.Millisecond)

	// Close the database connection
	err = underlyingDb.Close()
	if err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}

	// Remove from pool
	delete(c.pool, name)

	return nil
}

func (c *testDBPostgres) CloseAll() error {
	defer c.clean()
	var merr error
	for name := range c.pool {
		err := c.Close(name)
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}
	c.pool = nil
	return merr
}

func (c *testDBPostgres) DbType() string {
	return DBTypePostgres
}

const (
	postgresPassword = "password"
	postgresUser     = "testuser"
)

func (c *testDBPostgres) Init(logger logger.Interface) {
	c.logger = logger
	c.pool = map[string]*gorm.DB{}

	c.once.Do(func() {
		ctx := context.Background()

		req := testcontainers.ContainerRequest{
			Image:        "postgres:13",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     postgresUser,
				"POSTGRES_PASSWORD": postgresPassword,
				"POSTGRES_DB":       defaultDbName,
			},
			// postgres:13 starts a temporary server during initdb, then restarts
			// the real one. Waiting only for the port races into that init window.
			// "ready to accept connections" is logged twice: wait for the second.
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
			),
		}

		postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to start PostgreSQL container: %v", err))
		}

		host, err := postgresContainer.Host(ctx)
		if err != nil {
			panic(fmt.Sprintf("failed to get PostgreSQL container host: %v", err))
		}
		c.host = host

		port, err := postgresContainer.MappedPort(ctx, "5432")
		if err != nil {
			panic(fmt.Sprintf("failed to get PostgreSQL container port: %v", err))
		}
		c.port = port.Port()

		dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port.Port(), postgresUser, defaultDbName, postgresPassword)
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to connect to PostgreSQL test database: %v", err))
		}

		cleanFn := func() {
			if err := postgresContainer.Terminate(ctx); err != nil {
				panic(fmt.Sprintf("failed to terminate postgres container: %v", err))
			}
		}
		c.clean = cleanFn
		c.pool[normalizeDbName(defaultDbName)] = db
	})
}

func (c *testDBPostgres) Conn() *gorm.DB {
	return c.ConnDbName(defaultDbName)
}

func (c *testDBPostgres) ConnDbName(name string) *gorm.DB {
	name = normalizeDbName(name)
	dbConn, exists := c.pool[name]
	if exists {
		return dbConn
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s  sslmode=disable", c.host, c.port, postgresUser, postgresPassword, defaultDbName)
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: c.logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to PostgreSQL test database: %v", err))
	}

	createDatabaseCommand := fmt.Sprintf("CREATE DATABASE %s", name)
	admin.Exec(createDatabaseCommand)

	// Close the admin connection used only to create the database. Without this
	// its pool stays open for the life of the process, so creating many test
	// databases exhausts the server's connection limit ("too many clients
	// already"). MySQL's ConnDbName already does this via defer.
	if sqlDB, err := admin.DB(); err == nil {
		_ = sqlDB.Close()
	}

	dsn = fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", c.host, c.port, postgresUser, name, postgresPassword)
	gormDb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: c.logger,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to PostgreSQL test database: %v", err))
	}

	c.pool[name] = gormDb
	return gormDb

}
