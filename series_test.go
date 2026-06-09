package timeseries

import (
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

func TestDefineSeries(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestDefineSeries"))
			if err != nil {
				t.Fatal(err)
			}

			cfg := Series{Name: "AAPL", Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour}
			if err := s.DefineSeries(cfg); err != nil {
				t.Fatalf("DefineSeries: %v", err)
			}

			got, err := s.GetSeries("AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if got != cfg {
				t.Fatalf("GetSeries = %+v, want %+v", got, cfg)
			}

			list, err := s.ListSeries()
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 {
				t.Fatalf("ListSeries len = %d, want 1", len(list))
			}

			if err := s.DropSeries("AAPL"); err != nil {
				t.Fatalf("DropSeries: %v", err)
			}
			if _, err := s.GetSeries("AAPL"); err == nil {
				t.Fatal("expected error after DropSeries, got nil")
			}
		})
	}
}

func TestDefineSeries_Validation(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestDefineSeriesBad"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DefineSeries(Series{Name: "x", Precision: 0, Retention: time.Hour}); err == nil {
		t.Fatal("expected error for zero precision")
	}
	if err := s.DefineSeries(Series{Name: "x", Precision: time.Millisecond, Retention: time.Hour}); err == nil {
		t.Fatal("expected error for sub-second precision")
	}
}
