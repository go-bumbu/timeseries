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
			s, err := New(tdb.ConnDbName("TestDefineSeries"))
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

func TestDefineSeries_Validation(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestDefineSeriesBad"))
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
