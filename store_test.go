package timeseries

import (
	"context"
	"errors"
	"fmt"
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

// TestConcurrentAccess exercises the Store's internal serialization under real
// contention: stable-field writers, a writer of a field that is repeatedly
// dropped and re-added, readers, a definer that toggles that field (cascading
// its records), and a maintainer all run on one Store. It then asserts the
// data invariants the lock exists to protect — no orphan records survive a
// concurrent field-drop, and reduction is consistent — not merely that nothing
// errored. Run with -race for full value.
//
// Pinned to the SQLite backend with a single connection: this isolates the
// Go-level lock discipline from driver busy-retry semantics. Concurrent-write
// behavior on PostgreSQL/MySQL is not exercised here.
func TestConcurrentAccess(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestConcurrent"))
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := s.db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	ctx := context.Background()
	const long = 100 * 365 * 24 * time.Hour
	// defWith carries an extra "tmp" field; defWithout drops it (cascading its
	// records). The definer toggles between the two, so a writer resolving
	// "tmp" can race the cascade — the exact orphan-record window the lock closes.
	defWith := Series{Name: "C", Precision: time.Hour, Retention: long,
		Fields: []Field{{Name: "v", Aggregate: AggMax}, {Name: "tmp", Aggregate: AggMax}}}
	defWithout := Series{Name: "C", Precision: time.Hour, Retention: long,
		Fields: []Field{{Name: "v", Aggregate: AggMax}}}
	if err := s.DefineSeries(ctx, defWith); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	const iters = 50
	var errs []error
	var errMu sync.Mutex
	report := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	// stable-field writers: "v" always exists, so these must always succeed.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				ts := base.Add(time.Duration(w*1000+i) * time.Minute)
				report(s.Write(ctx, "C", Point{Time: ts, Values: map[string]float64{"v": float64(i)}}))
			}
		}(w)
	}
	// dropped-field writer: "tmp" may be absent, so ErrFieldNotFound is a
	// legitimate outcome of the race and is tolerated; anything else is a bug.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			ts := base.Add(time.Duration(5000+i) * time.Minute)
			err := s.Write(ctx, "C", Point{Time: ts, Values: map[string]float64{"tmp": float64(i)}})
			if err != nil && !errors.Is(err, ErrFieldNotFound) {
				report(err)
			}
		}
	}()
	// readers: a pivoted Point must never contain an orphan (empty-name) field,
	// which is exactly what a record pointing at a dropped field id would yield.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				pts, err := s.Range(ctx, "C", time.Time{}, time.Time{})
				if err != nil {
					report(err)
					continue
				}
				for _, p := range pts {
					if _, orphan := p.Values[""]; orphan {
						report(fmt.Errorf("orphan record observed: %+v", p.Values))
					}
				}
			}
		}()
	}
	// definer toggles the "tmp" field; maintainer reduces concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if i%2 == 0 {
				report(s.DefineSeries(ctx, defWithout))
			} else {
				report(s.DefineSeries(ctx, defWith))
			}
			report(s.Maintain(ctx))
		}
	}()

	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent operations failed (%d): %v", len(errs), errs)
	}

	// Settle to a known schema (drops "tmp" and its records) and reduce.
	if err := s.DefineSeries(ctx, defWithout); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}

	// No orphan records: every surviving record's field resolves to a live name.
	pts, err := s.Range(ctx, "C", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) == 0 {
		t.Fatal("expected surviving points for field v")
	}
	for _, p := range pts {
		if _, orphan := p.Values[""]; orphan {
			t.Fatalf("orphan record survived settle: %+v", p.Values)
		}
		if _, ok := p.Values["tmp"]; ok {
			t.Fatalf("dropped field tmp still present after settle: %+v", p.Values)
		}
	}

	// Reduction is settled: a second Maintain changes nothing (idempotent).
	var before, after int64
	if err := s.db.Model(&dbRecord{}).Count(&before).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Model(&dbRecord{}).Count(&after).Error; err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("Maintain not idempotent after settle: %d -> %d rows", before, after)
	}
}
