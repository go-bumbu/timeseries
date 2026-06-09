package timeseries

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestNew_NilDB(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) should return an error, not panic")
	}
}

// TestContextCancellation verifies a canceled context aborts a query rather than
// running it. Read and write paths must both honor it.
func TestContextCancellation(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestCtxCancel"))
			if err != nil {
				t.Fatal(err)
			}
			setupAAPL(t, s)
			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // canceled before use

			if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{"close": 1}}); !errors.Is(err, context.Canceled) {
				t.Fatalf("Write with canceled ctx = %v, want context.Canceled", err)
			}
			if _, err := s.Range(ctx, "AAPL", time.Time{}, time.Time{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("Range with canceled ctx = %v, want context.Canceled", err)
			}
		})
	}
}

// TestConcurrentAccess exercises the Store's internal serialization: many
// readers and writers run alongside DefineSeries and Maintain on the same
// Store. It must not deadlock or trip the race detector, and every operation
// against the live series must succeed. Run with -race for full value.
func TestConcurrentAccess(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestConcurrent"))
	if err != nil {
		t.Fatal(err)
	}
	// Serialize at the driver so concurrent SQLite writers don't hit SQLITE_BUSY;
	// the point of this test is our locking discipline, not driver concurrency.
	if sqlDB, err := s.db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	ctx := context.Background()
	def := Series{
		Name: "C", Precision: time.Hour, Retention: 100 * 365 * 24 * time.Hour,
		Fields: []Field{{Name: "v", Aggregate: AggMax}},
	}
	if err := s.DefineSeries(ctx, def); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	const iters = 50
	errCh := make(chan error, 8)
	report := func(err error) {
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}

	var wg sync.WaitGroup
	// writers
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				ts := base.Add(time.Duration(w*iters+i) * time.Minute)
				report(s.Write(ctx, "C", Point{Time: ts, Values: map[string]float64{"v": float64(i)}}))
			}
		}(w)
	}
	// readers
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if _, err := s.Range(ctx, "C", time.Time{}, time.Time{}); err != nil {
					report(err)
				}
			}
		}()
	}
	// a structural worker: redefines (keeping the field) and maintains
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			report(s.DefineSeries(ctx, def))
			report(s.Maintain(ctx))
		}
	}()

	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		t.Fatalf("concurrent operation failed: %v", err)
	}
}
