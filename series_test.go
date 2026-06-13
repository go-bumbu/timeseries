package timeseries

import (
	"context"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

func TestDefineSeries(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeries"))
			if err != nil {
				t.Fatal(err)
			}

			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
			}
			if err := s.DefineSeries(context.Background(), cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}

			got, err := s.GetSeries(context.Background(), "AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(cfg, got); diff != "" {
				t.Fatalf("GetSeries mismatch (-want +got):\n%s", diff)
			}

			list, err := s.ListSeries(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 {
				t.Fatalf("ListSeries len = %d, want 1", len(list))
			}

			if err := s.DropSeries(context.Background(), "AAPL"); err != nil {
				t.Fatalf("DropSeries: %v", err)
			}
			if _, err := s.GetSeries(context.Background(), "AAPL"); err == nil {
				t.Fatal("expected error after DropSeries, got nil")
			}
		})
	}
}

// TestDefineSeries_UnchangedSkipsWriteLock asserts that redefining a series
// with an identical definition is a no-op that takes only the read lock — it
// must not block on the exclusive lock. The test holds the read lock and
// requires the redundant DefineSeries to still complete: an exclusive-lock
// acquisition would deadlock against the held RLock and time out.
func TestDefineSeries_UnchangedSkipsWriteLock(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesUnchanged"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}, {Name: "volume", Aggregate: AggSum}},
			}
			if err := s.DefineSeries(ctx, cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}

			// Hold the read lock: a redundant define must not need the write lock.
			s.mu.RLock()
			defer s.mu.RUnlock()
			done := make(chan error, 1)
			go func() { done <- s.DefineSeries(ctx, cfg) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("redundant DefineSeries: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("redundant DefineSeries blocked on the exclusive lock")
			}
		})
	}
}

// TestDefineSeries_ChangeStillApplies guards the escalation path: when the
// requested definition differs from what is stored (retention, field set), the
// full define-and-reconcile must still run.
func TestDefineSeries_ChangeStillApplies(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesChange"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := s.DefineSeries(ctx, Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
			}); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}

			// Change retention and add a field.
			changed := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 60 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}, {Name: "volume", Aggregate: AggSum}},
			}
			if err := s.DefineSeries(ctx, changed); err != nil {
				t.Fatalf("DefineSeries (change): %v", err)
			}
			got, err := s.GetSeries(ctx, "AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(changed, got); diff != "" {
				t.Fatalf("GetSeries mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDefineSeries_Validation(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesBad"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := s.DefineSeries(ctx, Series{Name: "x", Precision: 0, Retention: time.Hour}); err == nil {
				t.Fatal("expected error for zero precision")
			}
			if err := s.DefineSeries(ctx, Series{Name: "x", Precision: time.Millisecond, Retention: time.Hour}); err == nil {
				t.Fatal("expected error for sub-second precision")
			}
		})
	}
}
