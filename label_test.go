package timeseries

import (
	"context"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

// TestSeriesLabelsTableMigrated asserts New creates the series_labels table.
func TestSeriesLabelsTableMigrated(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestSeriesLabelsTableMigrated"))
			if err != nil {
				t.Fatal(err)
			}
			if !s.db.Migrator().HasTable(&dbSeriesLabel{}) {
				t.Fatal("series_labels table was not migrated")
			}
		})
	}
}

// TestDefineSeries_RoundTripsLabels asserts labels survive Define -> GetSeries.
func TestDefineSeries_RoundTripsLabels(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesRoundTripsLabels"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
				Labels: map[string]string{"type": "price", "symbol": "AAPL"},
			}
			if err := s.DefineSeries(ctx, cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}
			got, err := s.GetSeries(ctx, "AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(cfg, got); diff != "" {
				t.Fatalf("GetSeries mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDefineSeries_RejectsEmptyLabelKey asserts an empty key is rejected.
func TestDefineSeries_RejectsEmptyLabelKey(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesRejectsEmptyLabelKey"))
			if err != nil {
				t.Fatal(err)
			}
			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
				Labels: map[string]string{"": "oops"},
			}
			if err := s.DefineSeries(context.Background(), cfg); err == nil {
				t.Fatal("expected error for empty label key, got nil")
			}
		})
	}
}

// TestDefineSeries_LabelsDeclarativeSync asserts re-defining adds/updates/removes labels.
func TestDefineSeries_LabelsDeclarativeSync(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesLabelsDeclarativeSync"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			base := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
				Labels: map[string]string{"type": "price", "drop": "me"},
			}
			if err := s.DefineSeries(ctx, base); err != nil {
				t.Fatalf("DefineSeries base: %v", err)
			}
			// Update "type"'s value, remove "drop", add "symbol".
			next := base
			next.Labels = map[string]string{"type": "equity", "symbol": "AAPL"}
			if err := s.DefineSeries(ctx, next); err != nil {
				t.Fatalf("DefineSeries next: %v", err)
			}
			got, err := s.GetSeries(ctx, "AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(next.Labels, got.Labels); diff != "" {
				t.Fatalf("labels mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDefineSeries_UnchangedWithLabelsSkipsWriteLock asserts the fast path
// accounts for labels: re-defining a series with identical fields AND identical
// labels stays a read-lock no-op. The test holds the read lock; an exclusive-lock
// escalation would deadlock and time out. (Mirrors the field-only fast-path test.)
func TestDefineSeries_UnchangedWithLabelsSkipsWriteLock(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDefineSeriesUnchangedWithLabels"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
				Labels: map[string]string{"type": "price", "symbol": "AAPL"},
			}
			if err := s.DefineSeries(ctx, cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}
			s.mu.RLock()
			defer s.mu.RUnlock()
			done := make(chan error, 1)
			go func() { done <- s.DefineSeries(ctx, cfg) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("redundant labeled DefineSeries: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("redundant labeled DefineSeries blocked on the exclusive lock")
			}
		})
	}
}

// TestListSeries_FilterByLabel asserts MatchLabel filters and labels are returned.
func TestListSeries_FilterByLabel(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestListSeriesFilterByLabel"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			mk := func(name, typ string) Series {
				return Series{
					Name: name, Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
					Fields: []Field{{Name: "v", Aggregate: AggLast}},
					Labels: map[string]string{"type": typ, "env": "prod"},
				}
			}
			for _, cfg := range []Series{mk("a", "price"), mk("b", "price"), mk("c", "fx")} {
				if err := s.DefineSeries(ctx, cfg); err != nil {
					t.Fatalf("DefineSeries %s: %v", cfg.Name, err)
				}
			}

			// Single matcher.
			price, err := s.ListSeries(ctx, MatchLabel("type", "price"))
			if err != nil {
				t.Fatal(err)
			}
			if len(price) != 2 {
				t.Fatalf("type=price: got %d series, want 2", len(price))
			}
			if price[0].Labels["env"] != "prod" {
				t.Fatalf("labels not populated on list result: %+v", price[0].Labels)
			}

			// Two matchers AND together.
			both, err := s.ListSeries(ctx, MatchLabel("type", "price"), MatchLabel("env", "prod"))
			if err != nil {
				t.Fatal(err)
			}
			if len(both) != 2 {
				t.Fatalf("type=price AND env=prod: got %d, want 2", len(both))
			}

			// No match -> empty.
			none, err := s.ListSeries(ctx, MatchLabel("type", "nope"))
			if err != nil {
				t.Fatal(err)
			}
			if len(none) != 0 {
				t.Fatalf("type=nope: got %d, want 0", len(none))
			}

			// Zero options -> all (backward compatible).
			all, err := s.ListSeries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(all) != 3 {
				t.Fatalf("no filter: got %d, want 3", len(all))
			}
		})
	}
}

// TestDropSeries_RemovesLabels asserts DropSeries and Wipe leave no orphan labels.
func TestDropSeries_RemovesLabels(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDropSeriesRemovesLabels"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			cfg := Series{
				Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour,
				Fields: []Field{{Name: "close", Aggregate: AggLast}},
				Labels: map[string]string{"type": "price"},
			}
			if err := s.DefineSeries(ctx, cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}

			countLabels := func() int64 {
				var n int64
				if err := s.db.Model(&dbSeriesLabel{}).Count(&n).Error; err != nil {
					t.Fatalf("count labels: %v", err)
				}
				return n
			}

			if err := s.DropSeries(ctx, "AAPL"); err != nil {
				t.Fatalf("DropSeries: %v", err)
			}
			if n := countLabels(); n != 0 {
				t.Fatalf("after DropSeries: %d orphan labels, want 0", n)
			}

			// Re-create then Wipe.
			if err := s.DefineSeries(ctx, cfg); err != nil {
				t.Fatalf("DefineSeries (2): %v", err)
			}
			if err := s.Wipe(ctx); err != nil {
				t.Fatalf("Wipe: %v", err)
			}
			if n := countLabels(); n != 0 {
				t.Fatalf("after Wipe: %d orphan labels, want 0", n)
			}
		})
	}
}
