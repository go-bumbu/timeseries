package timeseries

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

func setupAAPL(t *testing.T, s *Store) {
	t.Helper()
	if err := s.DefineSeries(context.Background(), Series{
		Name:      "AAPL",
		Precision: 24 * time.Hour,
		Retention: 365 * 24 * time.Hour,
		Fields: []Field{
			{Name: "open", Aggregate: AggFirst},
			{Name: "close", Aggregate: AggLast},
			{Name: "volume", Aggregate: AggSum},
		},
	}); err != nil {
		t.Fatal(err)
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

			if err := s.Write(context.Background(), "AAPL", Point{Time: day, Values: map[string]float64{
				"open": 100, "close": 101, "volume": 1000,
			}}); err != nil {
				t.Fatalf("Write: %v", err)
			}
			// re-write same timestamp: must overwrite, not duplicate
			if err := s.Write(context.Background(), "AAPL", Point{Time: day, Values: map[string]float64{
				"open": 100, "close": 105, "volume": 2000,
			}}); err != nil {
				t.Fatalf("Write 2: %v", err)
			}

			var count int64
			if err := s.db.Model(&dbRecord{}).Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			if count != 3 {
				t.Fatalf("row count = %d, want 3 (upsert must not duplicate)", count)
			}

			v, found, err := s.FieldAt(context.Background(), "AAPL", "close", day)
			if err != nil || !found {
				t.Fatalf("FieldAt: v=%v found=%v err=%v", v, found, err)
			}
			if v != 105 {
				t.Fatalf("close = %v, want 105 (overwritten)", v)
			}
		})
	}
}

// TestMove_Relocate verifies Move deletes the record at oldTime and upserts the
// new point at its time in one shot — relocating a record to a new timestamp,
// with possibly changed values, and leaving nothing behind at the old time.
func TestMove_Relocate(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMoveRelocate"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			day1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			day2 := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
			if err := s.Write(ctx, "AAPL", Point{Time: day1, Values: map[string]float64{"open": 100, "close": 101, "volume": 1000}}); err != nil {
				t.Fatal(err)
			}

			// Move to day2 with new values.
			if err := s.Move(ctx, "AAPL", day1, Point{Time: day2, Values: map[string]float64{"open": 200, "close": 202, "volume": 2000}}); err != nil {
				t.Fatalf("Move: %v", err)
			}

			pts, err := s.Range(ctx, "AAPL", time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(pts) != 1 {
				t.Fatalf("after Move: %d points, want 1 (old time must be gone)", len(pts))
			}
			if !pts[0].Time.Equal(day2) {
				t.Fatalf("after Move: point at %v, want %v", pts[0].Time, day2)
			}
			if pts[0].Values["close"] != 202 || pts[0].Values["open"] != 200 {
				t.Fatalf("after Move: values = %v, want open=200 close=202", pts[0].Values)
			}
		})
	}
}

// TestMove_ZeroTime rejects a new point with a zero timestamp, mirroring Write.
func TestMove_ZeroTime(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMoveZeroTime"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			day1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			if err := s.Move(ctx, "AAPL", day1, Point{Values: map[string]float64{"close": 1}}); err == nil {
				t.Fatal("Move with zero point time should error")
			}
		})
	}
}

func TestWrite_Errors(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestWriteErrors"))
			if err != nil {
				t.Fatal(err)
			}
			setupAAPL(t, s)
			ctx := context.Background()

			err = s.Write(ctx, "NOPE", Point{Time: time.Now(), Values: map[string]float64{"close": 1}})
			if !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("unknown series error = %v, want ErrSeriesNotFound", err)
			}
			err = s.Write(ctx, "AAPL", Point{Time: time.Now(), Values: map[string]float64{"ghost": 1}})
			if !errors.Is(err, ErrFieldNotFound) {
				t.Fatalf("undefined field error = %v, want ErrFieldNotFound", err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: time.Time{}, Values: map[string]float64{"close": 1}}); err == nil {
				t.Fatal("expected error for zero time")
			}
			if err := s.WriteMany(ctx, "AAPL", nil); err != nil {
				t.Fatalf("empty WriteMany should be no-op, got %v", err)
			}
		})
	}
}

// TestReadErrors_MissingSeries verifies every read/delete path returns the
// ErrSeriesNotFound sentinel (errors.Is testable) for an unknown series.
func TestReadErrors_MissingSeries(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestReadErrMissing"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now()

			if _, err := s.Range(ctx, "NOPE", time.Time{}, time.Time{}); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("Range err = %v, want ErrSeriesNotFound", err)
			}
			if _, err := s.FieldRange(ctx, "NOPE", "v", time.Time{}, time.Time{}); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("FieldRange err = %v, want ErrSeriesNotFound", err)
			}
			if _, _, err := s.FieldAt(ctx, "NOPE", "v", now); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("FieldAt err = %v, want ErrSeriesNotFound", err)
			}
			if _, err := s.At(ctx, "NOPE", now); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("At err = %v, want ErrSeriesNotFound", err)
			}
			if err := s.Delete(ctx, "NOPE", now); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("Delete err = %v, want ErrSeriesNotFound", err)
			}
			if err := s.DeleteRange(ctx, "NOPE", time.Time{}, time.Time{}); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("DeleteRange err = %v, want ErrSeriesNotFound", err)
			}
			if err := s.DropSeries(ctx, "NOPE"); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("DropSeries err = %v, want ErrSeriesNotFound", err)
			}
			if _, err := s.GetSeries(ctx, "NOPE"); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("GetSeries err = %v, want ErrSeriesNotFound", err)
			}
		})
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
		if err := s.Write(context.Background(), "AAPL", Point{Time: d, Values: map[string]float64{
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
			pts, err := s.Range(context.Background(), "AAPL", d1, d2)
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
			closes, err := s.FieldRange(context.Background(), "AAPL", "close", d1, d3)
			if err != nil {
				t.Fatal(err)
			}
			if len(closes) != 3 || closes[0].Value != 101 || closes[2].Value != 105 {
				t.Fatalf("FieldRange close = %+v", closes)
			}
		})
	}
}

// TestOpenEndedRanges exercises the documented zero-time unbounded bounds on
// Range, FieldRange and DeleteRange across all DBs (zero time must omit the
// bound, not filter on NULL).
func TestOpenEndedRanges(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestOpenEnded"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			d1, d2, _ := readFixture(t, s)

			// Fully unbounded Range: all three points.
			pts, err := s.Range(ctx, "AAPL", time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(pts) != 3 {
				t.Fatalf("unbounded Range len = %d, want 3", len(pts))
			}

			// Unbounded start, bounded end: points up to and including d1.
			closes, err := s.FieldRange(ctx, "AAPL", "close", time.Time{}, d1)
			if err != nil {
				t.Fatal(err)
			}
			if len(closes) != 1 || closes[0].Value != 101 {
				t.Fatalf("FieldRange(.., d1) = %+v, want one sample 101", closes)
			}

			// Bounded start, unbounded end: points from d2 onward.
			closes, err = s.FieldRange(ctx, "AAPL", "close", d2, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(closes) != 2 {
				t.Fatalf("FieldRange(d2, ..) len = %d, want 2", len(closes))
			}

			// Unbounded start DeleteRange removes d1 and d2, leaving d3.
			if err := s.DeleteRange(ctx, "AAPL", time.Time{}, d2); err != nil {
				t.Fatal(err)
			}
			rest, err := s.Range(ctx, "AAPL", time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(rest) != 1 {
				t.Fatalf("after open-ended DeleteRange, points = %d, want 1 (d3 only)", len(rest))
			}
		})
	}
}

// TestAt_DivergentTimestamps is the case At exists for: fields whose latest
// values land at different source times. The snapshot must pick each field's
// own latest value at or before t, independently.
func TestAt_DivergentTimestamps(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestAtDivergent"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			d1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			d2 := d1.Add(24 * time.Hour)
			d3 := d2.Add(24 * time.Hour)

			// open last written at d1; close last written at d2; volume at d3.
			if err := s.Write(ctx, "AAPL", Point{Time: d1, Values: map[string]float64{"open": 100}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: d2, Values: map[string]float64{"close": 200}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: d3, Values: map[string]float64{"volume": 300}}); err != nil {
				t.Fatal(err)
			}

			// As of d3: each field resolves to its own latest <= d3.
			snap, err := s.At(ctx, "AAPL", d3)
			if err != nil {
				t.Fatal(err)
			}
			if snap.Values["open"] != 100 || snap.Values["close"] != 200 || snap.Values["volume"] != 300 {
				t.Fatalf("At(d3) divergent snapshot = %+v, want open=100 close=200 volume=300", snap.Values)
			}

			// As of d2: volume (only at d3) must be absent; open/close present.
			snap2, err := s.At(ctx, "AAPL", d2)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := snap2.Values["volume"]; ok {
				t.Fatalf("At(d2) should not include volume (first written at d3): %+v", snap2.Values)
			}
			if snap2.Values["open"] != 100 || snap2.Values["close"] != 200 {
				t.Fatalf("At(d2) = %+v, want open=100 close=200", snap2.Values)
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
			v, found, err := s.FieldAt(context.Background(), "AAPL", "close", d2.Add(time.Hour))
			if err != nil || !found || v != 103 {
				t.Fatalf("FieldAt = %v found=%v err=%v, want 103", v, found, err)
			}
			if _, found, _ := s.FieldAt(context.Background(), "AAPL", "close", d1.Add(-time.Hour)); found {
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
			snap, err := s.At(context.Background(), "AAPL", d2.Add(time.Hour))
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
				if err := s.Write(context.Background(), "AAPL", Point{Time: d, Values: map[string]float64{"close": 1, "open": 1}}); err != nil {
					t.Fatal(err)
				}
			}

			// Delete one point (all its fields)
			if err := s.Delete(context.Background(), "AAPL", d2); err != nil {
				t.Fatal(err)
			}
			var c int64
			if err := s.db.Model(&dbRecord{}).Where("time = ?", unixMilli(d2)).Count(&c).Error; err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("after Delete(d2) count = %d, want 0", c)
			}

			// DeleteRange removes d1 (and would remove d2 if present)
			if err := s.DeleteRange(context.Background(), "AAPL", d1, d2); err != nil {
				t.Fatal(err)
			}
			var total int64
			if err := s.db.Model(&dbRecord{}).Count(&total).Error; err != nil {
				t.Fatal(err)
			}
			if total != 2 { // only d3's two fields remain
				t.Fatalf("after DeleteRange count = %d, want 2", total)
			}

			// DropSeries cascade
			if err := s.DropSeries(context.Background(), "AAPL"); err != nil {
				t.Fatal(err)
			}
			if err := s.db.Model(&dbRecord{}).Count(&total).Error; err != nil {
				t.Fatal(err)
			}
			if total != 0 {
				t.Fatalf("after DropSeries cascade count = %d, want 0", total)
			}
			var fieldsLeft int64
			if err := s.db.Model(&dbField{}).Count(&fieldsLeft).Error; err != nil {
				t.Fatal(err)
			}
			if fieldsLeft != 0 {
				t.Fatalf("after DropSeries field count = %d, want 0 (fields must cascade)", fieldsLeft)
			}
		})
	}
}

func TestPerSeriesFieldIndependence(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestPerSeriesFieldIndep"))
			if err != nil {
				t.Fatal(err)
			}
			long := 100 * 365 * 24 * time.Hour
			if err := s.DefineSeries(context.Background(), Series{
				Name: "A", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "v", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.DefineSeries(context.Background(), Series{
				Name: "B", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "v", Aggregate: AggMin}},
			}); err != nil {
				t.Fatal(err)
			}

			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			write := func(series string) {
				if err := s.WriteMany(context.Background(), series, []Point{
					{Time: day.Add(9 * time.Hour), Values: map[string]float64{"v": 10}},
					{Time: day.Add(16 * time.Hour), Values: map[string]float64{"v": 20}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			write("A")
			write("B")

			if err := s.Maintain(context.Background()); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			va, _, _ := s.FieldAt(context.Background(), "A", "v", day.Add(24*time.Hour))
			vb, _, _ := s.FieldAt(context.Background(), "B", "v", day.Add(24*time.Hour))
			if va != 20 {
				t.Fatalf("A.v = %v, want 20 (max)", va)
			}
			if vb != 10 {
				t.Fatalf("B.v = %v, want 10 (min)", vb)
			}
		})
	}
}
