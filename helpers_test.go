package timeseries

import (
	"testing"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

// connDB opens a uniquely-named test database and closes its connection pool
// when the (sub)test ends. Each tdb.ConnDbName otherwise leaves its pool open
// for the whole run; across the full matrix that accumulates until the backend
// refuses new connections (Postgres "too many clients already"). Closing per
// test bounds the peak to the unavoidable per-database admin connection that
// testdbs opens to CREATE DATABASE and never closes — which we cannot reach
// from here.
func connDB(t *testing.T, tdb testdbs.TargetDb, name string) *gorm.DB {
	t.Helper()
	db := tdb.ConnDbName(name)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}
