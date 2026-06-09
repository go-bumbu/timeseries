package timeseries

import (
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

func setupAAPL(t *testing.T, s *Store) {
	t.Helper()
	if err := s.DefineSeries(Series{Name: "AAPL", Precision: 24 * time.Hour, Retention: 365 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []Field{
		{Name: "open", Aggregate: AggFirst},
		{Name: "close", Aggregate: AggLast},
		{Name: "volume", Aggregate: AggSum},
	} {
		if err := s.DefineField(f); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWrite_Upsert(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestWriteUpsert"))
			if err != nil {
				t.Fatal(err)
			}
			setupAAPL(t, s)
			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

			if err := s.Write("AAPL", Point{Time: day, Values: map[string]float64{
				"open": 100, "close": 101, "volume": 1000,
			}}); err != nil {
				t.Fatalf("Write: %v", err)
			}
			// re-write same timestamp: must overwrite, not duplicate
			if err := s.Write("AAPL", Point{Time: day, Values: map[string]float64{
				"open": 100, "close": 105, "volume": 2000,
			}}); err != nil {
				t.Fatalf("Write 2: %v", err)
			}

			var count int64
			s.db.Model(&dbRecord{}).Count(&count)
			if count != 3 {
				t.Fatalf("row count = %d, want 3 (upsert must not duplicate)", count)
			}

			v, found, err := s.FieldAt("AAPL", "close", day)
			if err != nil || !found {
				t.Fatalf("FieldAt: v=%v found=%v err=%v", v, found, err)
			}
			if v != 105 {
				t.Fatalf("close = %v, want 105 (overwritten)", v)
			}
		})
	}
}

func TestWrite_Errors(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestWriteErrors"))
	if err != nil {
		t.Fatal(err)
	}
	setupAAPL(t, s)

	if err := s.Write("NOPE", Point{Time: time.Now(), Values: map[string]float64{"close": 1}}); err == nil {
		t.Fatal("expected error for unknown series")
	}
	if err := s.Write("AAPL", Point{Time: time.Now(), Values: map[string]float64{"ghost": 1}}); err == nil {
		t.Fatal("expected error for undefined field")
	}
	if err := s.Write("AAPL", Point{Time: time.Time{}, Values: map[string]float64{"close": 1}}); err == nil {
		t.Fatal("expected error for zero time")
	}
	if err := s.WriteMany("AAPL", nil); err != nil {
		t.Fatalf("empty WriteMany should be no-op, got %v", err)
	}
}

// readFixture spins up a fresh Store, defines the AAPL series, and writes the
// three daily points (d1/d2/d3) shared by all read tests. It returns the store
// and the three timestamps.
func readFixture(t *testing.T, s *Store) (d1, d2, d3 time.Time) {
	t.Helper()
	setupAAPL(t, s)

	d1 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 = d1.Add(24 * time.Hour)
	d3 = d2.Add(24 * time.Hour)

	mustWrite := func(d time.Time, open, closeV, vol float64) {
		if err := s.Write("AAPL", Point{Time: d, Values: map[string]float64{
			"open": open, "close": closeV, "volume": vol,
		}}); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(d1, 100, 101, 1000)
	mustWrite(d2, 102, 103, 1100)
	mustWrite(d3, 104, 105, 1200)
	return d1, d2, d3
}

func TestRange(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestRange"))
			if err != nil {
				t.Fatal(err)
			}
			d1, d2, _ := readFixture(t, s)

			// Range -> pivoted points
			pts, err := s.Range("AAPL", d1, d2)
			if err != nil {
				t.Fatal(err)
			}
			if len(pts) != 2 {
				t.Fatalf("Range len = %d, want 2", len(pts))
			}
			if !pts[0].Time.Equal(d1) || pts[0].Values["close"] != 101 || pts[0].Values["open"] != 100 {
				t.Fatalf("Range[0] = %+v", pts[0])
			}
		})
	}
}

func TestFieldRange(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestFieldRange"))
			if err != nil {
				t.Fatal(err)
			}
			d1, _, d3 := readFixture(t, s)

			// FieldRange -> scalar series
			closes, err := s.FieldRange("AAPL", "close", d1, d3)
			if err != nil {
				t.Fatal(err)
			}
			if len(closes) != 3 || closes[0].Value != 101 || closes[2].Value != 105 {
				t.Fatalf("FieldRange close = %+v", closes)
			}
		})
	}
}

func TestFieldAt(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestFieldAt"))
			if err != nil {
				t.Fatal(err)
			}
			d1, d2, _ := readFixture(t, s)

			// FieldAt -> latest <= t
			v, found, err := s.FieldAt("AAPL", "close", d2.Add(time.Hour))
			if err != nil || !found || v != 103 {
				t.Fatalf("FieldAt = %v found=%v err=%v, want 103", v, found, err)
			}
			if _, found, _ := s.FieldAt("AAPL", "close", d1.Add(-time.Hour)); found {
				t.Fatal("FieldAt before first point should be found=false")
			}
		})
	}
}

func TestAt(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestAt"))
			if err != nil {
				t.Fatal(err)
			}
			_, d2, _ := readFixture(t, s)

			// At -> as-of snapshot of all fields
			snap, err := s.At("AAPL", d2.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if snap.Values["close"] != 103 || snap.Values["open"] != 102 || snap.Values["volume"] != 1100 {
				t.Fatalf("At snapshot = %+v", snap.Values)
			}
		})
	}
}

func TestDeletes(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestDeletes"))
			if err != nil {
				t.Fatal(err)
			}
			setupAAPL(t, s)
			d1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			d2 := d1.Add(24 * time.Hour)
			d3 := d2.Add(24 * time.Hour)
			for _, d := range []time.Time{d1, d2, d3} {
				if err := s.Write("AAPL", Point{Time: d, Values: map[string]float64{"close": 1, "open": 1}}); err != nil {
					t.Fatal(err)
				}
			}

			// Delete one point (all its fields)
			if err := s.Delete("AAPL", d2); err != nil {
				t.Fatal(err)
			}
			var c int64
			s.db.Model(&dbRecord{}).Where("time = ?", unixMilli(d2)).Count(&c)
			if c != 0 {
				t.Fatalf("after Delete(d2) count = %d, want 0", c)
			}

			// DeleteRange removes d1 (and would remove d2 if present)
			if err := s.DeleteRange("AAPL", d1, d2); err != nil {
				t.Fatal(err)
			}
			var total int64
			s.db.Model(&dbRecord{}).Count(&total)
			if total != 2 { // only d3's two fields remain
				t.Fatalf("after DeleteRange count = %d, want 2", total)
			}

			// DropSeries cascade
			if err := s.DropSeries("AAPL"); err != nil {
				t.Fatal(err)
			}
			s.db.Model(&dbRecord{}).Count(&total)
			if total != 0 {
				t.Fatalf("after DropSeries cascade count = %d, want 0", total)
			}
		})
	}
}
