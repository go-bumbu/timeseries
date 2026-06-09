package timeseries

import (
	"os"
	"strings"
	"testing"

	"github.com/go-bumbu/testdbs"
)

// TestMain initializes the test DB matrix once for the whole package.
// By default only the in-process SQLite backend is registered; Postgres/MySQL
// are added when run with -alldbs or TESTDBS_ALL.
func TestMain(m *testing.M) {
	testdbs.InitDBS()
	os.Exit(m.Run())
}

func TestMigrations(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			db := tdb.ConnDbName("TestMigrations")
			s, err := New(db)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			for _, tbl := range []string{"series", "fields", "records"} {
				if !s.db.Migrator().HasTable(tbl) {
					t.Fatalf("missing table %q", tbl)
				}
			}

			if s.db.Name() == "sqlite" {
				var ddl string
				if err := s.db.Raw(
					`SELECT sql FROM sqlite_master WHERE type='table' AND name='records'`,
				).Scan(&ddl).Error; err != nil {
					t.Fatalf("read ddl: %v", err)
				}
				if !strings.Contains(ddl, "WITHOUT ROWID") {
					t.Fatalf("records table not WITHOUT ROWID: %s", ddl)
				}
			}
		})
	}
}

func TestBuiltinAggregates(t *testing.T) {
	in := []float64{3, 1, 4, 1, 5} // ascending time order, by contract
	cases := map[string]struct {
		name string
		want float64
	}{
		"avg":   {AggAvg, 2.8},
		"sum":   {AggSum, 14},
		"min":   {AggMin, 1},
		"max":   {AggMax, 5},
		"first": {AggFirst, 3},
		"last":  {AggLast, 5},
	}
	s := &Store{aggregates: make(map[string]AggregateFn)}
	s.registerBuiltins()
	for label, tc := range cases {
		fn, ok := s.aggregates[tc.name]
		if !ok {
			t.Fatalf("%s: aggregate %q not registered", label, tc.name)
		}
		if got := fn(in); got != tc.want {
			t.Fatalf("%s(%v) = %v, want %v", tc.name, in, got, tc.want)
		}
	}
}

func TestRegisterAggregate_Custom(t *testing.T) {
	s := &Store{aggregates: make(map[string]AggregateFn)}
	s.RegisterAggregate("count", func(v []float64) float64 { return float64(len(v)) })
	if got := s.aggregates["count"]([]float64{9, 9, 9}); got != 3 {
		t.Fatalf("count = %v, want 3", got)
	}
}
