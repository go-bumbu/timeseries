package timeseries

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
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
			s, err := New(connDB(t, tdb,"TestWriteUpsert"))
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

// TestLatest returns the point at the series' maximum timestamp with its real
// time preserved, and reports found=false for an existing-but-empty series.
func TestLatest(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestLatest"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)

			// Empty series: CoverageNone, no error.
			if _, cov, err := s.Latest(ctx, "AAPL"); err != nil || cov != CoverageNone {
				t.Fatalf("Latest on empty series: cov=%v err=%v, want CoverageNone nil", cov, err)
			}

			day1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			day2 := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
			if err := s.Write(ctx, "AAPL", Point{Time: day1, Values: map[string]float64{"open": 100, "close": 101, "volume": 1000}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: day2, Values: map[string]float64{"open": 200, "close": 202, "volume": 2000}}); err != nil {
				t.Fatal(err)
			}

			p, cov, err := s.Latest(ctx, "AAPL")
			if err != nil || cov != CoverageFull {
				t.Fatalf("Latest: cov=%v err=%v, want CoverageFull nil", cov, err)
			}
			if !p.Time.Equal(day2) {
				t.Fatalf("Latest time = %v, want %v (real timestamp, not query time)", p.Time, day2)
			}
			if p.Values["close"] != 202 || p.Values["open"] != 200 {
				t.Fatalf("Latest values = %v, want open=200 close=202", p.Values)
			}

			// Unknown series propagates ErrSeriesNotFound.
			if _, _, err := s.Latest(ctx, "NOPE"); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("Latest on unknown series err = %v, want ErrSeriesNotFound", err)
			}
		})
	}
}

// TestLatest_SingleStatementSnapshot guards that Latest reads the newest point in a
// single SQL statement against the records table, not two. The old implementation ran
// two queries (find the newest timestamp, then fetch that timestamp's fields) with a
// gap a concurrent write could slip into — returning a stale point (a newer timestamp
// landed between the queries) or a torn one. A single MAX(time) subquery read takes a
// consistent snapshot, closing that window.
func TestLatest_SingleStatementSnapshot(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestLatestSnapshot"))
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
			if err := s.Write(ctx, "AAPL", Point{Time: day2, Values: map[string]float64{"open": 200, "close": 202, "volume": 2000}}); err != nil {
				t.Fatal(err)
			}

			recordsTable := (dbRecord{}).TableName()
			var recordSelects int
			const cb = "count_record_selects"
			// Count only real round-trips against the records table. GORM also runs the
			// MAX(time) subquery's callbacks in DryRun to build its SQL for inlining;
			// those don't touch the database, so skip them.
			if err := s.db.Callback().Query().After("gorm:query").Register(cb, func(db *gorm.DB) {
				if db.Statement.Table == recordsTable && !db.DryRun {
					recordSelects++
				}
			}); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.db.Callback().Query().Remove(cb) }()

			p, cov, err := s.Latest(ctx, "AAPL")
			if err != nil || cov != CoverageFull {
				t.Fatalf("Latest: cov=%v err=%v, want CoverageFull nil", cov, err)
			}
			if !p.Time.Equal(day2) || p.Values["open"] != 200 || p.Values["close"] != 202 {
				t.Fatalf("Latest = {%v %v}, want day2 open=200 close=202", p.Time, p.Values)
			}
			if recordSelects != 1 {
				t.Fatalf("Latest issued %d SELECTs against %q, want 1 (single-statement snapshot; two queries race with concurrent writes)", recordSelects, recordsTable)
			}
		})
	}
}

// TestLatestField returns the newest (time, value) for one field, with the real
// timestamp, and found=false when the field has no samples.
func TestLatestField(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestLatestField"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)

			if _, found, err := s.LatestField(ctx, "AAPL", "close"); err != nil || found {
				t.Fatalf("LatestField on empty series: found=%v err=%v, want false nil", found, err)
			}

			day1 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			day2 := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
			if err := s.Write(ctx, "AAPL", Point{Time: day1, Values: map[string]float64{"close": 101}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: day2, Values: map[string]float64{"close": 202}}); err != nil {
				t.Fatal(err)
			}

			sm, found, err := s.LatestField(ctx, "AAPL", "close")
			if err != nil || !found {
				t.Fatalf("LatestField: found=%v err=%v, want true nil", found, err)
			}
			if !sm.Time.Equal(day2) || sm.Value != 202 {
				t.Fatalf("LatestField = {%v, %v}, want {%v, 202}", sm.Time, sm.Value, day2)
			}

			if _, _, err := s.LatestField(ctx, "AAPL", "nope"); !errors.Is(err, ErrFieldNotFound) {
				t.Fatalf("LatestField unknown field err = %v, want ErrFieldNotFound", err)
			}
		})
	}
}

// TestCount returns the number of distinct timestamps (points) in a series,
// independent of how many fields each point carries.
func TestCount(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestCount"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)

			if n, err := s.Count(ctx, "AAPL"); err != nil || n != 0 {
				t.Fatalf("Count on empty series = %d, %v, want 0 nil", n, err)
			}

			// 3 points, each with multiple fields: count is 3 (distinct timestamps),
			// not 3*fields rows.
			for i := 0; i < 3; i++ {
				day := time.Date(2025, 1, 2+i, 0, 0, 0, 0, time.UTC)
				if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{"open": 1, "close": 2, "volume": 3}}); err != nil {
					t.Fatal(err)
				}
			}
			n, err := s.Count(ctx, "AAPL")
			if err != nil {
				t.Fatal(err)
			}
			if n != 3 {
				t.Fatalf("Count = %d, want 3 (distinct timestamps)", n)
			}
		})
	}
}

// TestCountAll returns distinct-timestamp counts per series in one query,
// includes zero-record series, and honors MatchLabel filters like ListSeries.
func TestCountAll(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestCountAll"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			mk := func(name, typ string) Series {
				return Series{
					Name: name, Precision: 24 * time.Hour, Retention: 365 * 24 * time.Hour,
					Fields: []Field{{Name: "open", Aggregate: AggFirst}, {Name: "close", Aggregate: AggLast}},
					Labels: map[string]string{"type": typ},
				}
			}
			for _, cfg := range []Series{mk("AAPL", "price"), mk("MSFT", "price"), mk("EURUSD", "fx")} {
				if err := s.DefineSeries(ctx, cfg); err != nil {
					t.Fatalf("DefineSeries %s: %v", cfg.Name, err)
				}
			}
			// AAPL: 3 points, each with multiple fields → still 3 distinct timestamps.
			for i := 0; i < 3; i++ {
				day := time.Date(2025, 1, 2+i, 0, 0, 0, 0, time.UTC)
				if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{"open": 1, "close": 2}}); err != nil {
					t.Fatal(err)
				}
			}
			// EURUSD: 1 point. MSFT: no records (must still appear with count 0).
			if err := s.Write(ctx, "EURUSD", Point{Time: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Values: map[string]float64{"close": 1}}); err != nil {
				t.Fatal(err)
			}

			// No options -> every series, zero-record series included.
			all, err := s.CountAll(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if want := map[string]int{"AAPL": 3, "MSFT": 0, "EURUSD": 1}; !reflect.DeepEqual(all, want) {
				t.Fatalf("CountAll() = %v, want %v", all, want)
			}

			// MatchLabel restricts the set like ListSeries.
			price, err := s.CountAll(ctx, MatchLabel("type", "price"))
			if err != nil {
				t.Fatal(err)
			}
			if want := map[string]int{"AAPL": 3, "MSFT": 0}; !reflect.DeepEqual(price, want) {
				t.Fatalf("CountAll(type=price) = %v, want %v", price, want)
			}

			// No match -> empty, non-nil.
			none, err := s.CountAll(ctx, MatchLabel("type", "nope"))
			if err != nil {
				t.Fatal(err)
			}
			if len(none) != 0 {
				t.Fatalf("CountAll(type=nope) = %v, want empty", none)
			}
		})
	}
}

func TestWrite_Errors(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestWriteErrors"))
			if err != nil {
				t.Fatal(err)
			}
			setupAAPL(t, s)
			ctx := context.Background()

			err = s.Write(ctx, "NOPE", Point{Time: time.Now().UTC(), Values: map[string]float64{"close": 1}})
			if !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("unknown series error = %v, want ErrSeriesNotFound", err)
			}
			err = s.Write(ctx, "AAPL", Point{Time: time.Now().UTC(), Values: map[string]float64{"ghost": 1}})
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
			s, err := New(connDB(t, tdb,"TestReadErrMissing"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now().UTC()

			if _, err := s.Range(ctx, "NOPE", time.Time{}, time.Time{}); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("Range err = %v, want ErrSeriesNotFound", err)
			}
			if _, err := s.FieldRange(ctx, "NOPE", "v", time.Time{}, time.Time{}); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("FieldRange err = %v, want ErrSeriesNotFound", err)
			}
			if _, _, err := s.FieldAt(ctx, "NOPE", "v", now); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("FieldAt err = %v, want ErrSeriesNotFound", err)
			}
			if _, _, err := s.At(ctx, "NOPE", now); !errors.Is(err, ErrSeriesNotFound) {
				t.Fatalf("At err = %v, want ErrSeriesNotFound", err)
			}
			if _, err := s.Delete(ctx, "NOPE", now); !errors.Is(err, ErrSeriesNotFound) {
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
			s, err := New(connDB(t, tdb,"TestRange"))
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
			s, err := New(connDB(t, tdb,"TestFieldRange"))
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
			s, err := New(connDB(t, tdb,"TestOpenEnded"))
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
			s, err := New(connDB(t, tdb,"TestAtDivergent"))
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

			// As of d3: each field resolves to its own latest <= d3 — full coverage.
			snap, cov, err := s.At(ctx, "AAPL", d3)
			if err != nil {
				t.Fatal(err)
			}
			if cov != CoverageFull {
				t.Fatalf("At(d3) coverage = %v, want CoverageFull (all fields resolve <= d3)", cov)
			}
			if snap.Values["open"] != 100 || snap.Values["close"] != 200 || snap.Values["volume"] != 300 {
				t.Fatalf("At(d3) divergent snapshot = %+v, want open=100 close=200 volume=300", snap.Values)
			}

			// As of d2: volume (only at d3) must be absent; open/close present — a
			// partial snapshot, reported as CoveragePartial.
			snap2, cov, err := s.At(ctx, "AAPL", d2)
			if err != nil {
				t.Fatal(err)
			}
			if cov != CoveragePartial {
				t.Fatalf("At(d2) coverage = %v, want CoveragePartial (volume first written at d3)", cov)
			}
			if _, ok := snap2.Values["volume"]; ok {
				t.Fatalf("At(d2) should not include volume (first written at d3): %+v", snap2.Values)
			}
			if snap2.Values["open"] != 100 || snap2.Values["close"] != 200 {
				t.Fatalf("At(d2) = %+v, want open=100 close=200", snap2.Values)
			}

			// Before the first point: nothing resolves, CoverageNone.
			if _, cov, err := s.At(ctx, "AAPL", d1.Add(-time.Hour)); err != nil || cov != CoverageNone {
				t.Fatalf("At(before d1) cov=%v err=%v, want CoverageNone", cov, err)
			}
		})
	}
}

func TestFieldAt(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestFieldAt"))
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
			s, err := New(connDB(t, tdb,"TestAt"))
			if err != nil {
				t.Fatal(err)
			}
			_, d2, _ := readFixture(t, s)

			// At -> as-of snapshot of all fields
			snap, cov, err := s.At(context.Background(), "AAPL", d2.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if cov != CoverageFull {
				t.Fatalf("At coverage = %v, want CoverageFull", cov)
			}
			if snap.Values["close"] != 103 || snap.Values["open"] != 102 || snap.Values["volume"] != 1100 {
				t.Fatalf("At snapshot = %+v", snap.Values)
			}
		})
	}
}

// TestCoverage_FieldAddedLater is the schema-evolution case: a field added to a
// series after data exists has no records at older timestamps, so both At and
// Latest report CoveragePartial there until that field is written.
func TestCoverage_FieldAddedLater(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestCoverageFieldAdded"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s) // fields: open, close, volume

			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
			if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{
				"open": 100, "close": 101, "volume": 1000,
			}}); err != nil {
				t.Fatal(err)
			}

			// All three defined fields present -> full, for both reads.
			if _, cov, err := s.At(ctx, "AAPL", day); err != nil || cov != CoverageFull {
				t.Fatalf("At before field add: cov=%v err=%v, want CoverageFull", cov, err)
			}
			if _, cov, err := s.Latest(ctx, "AAPL"); err != nil || cov != CoverageFull {
				t.Fatalf("Latest before field add: cov=%v err=%v, want CoverageFull", cov, err)
			}

			// Add a fourth field; the existing candle has no value for it.
			if err := s.DefineSeries(ctx, Series{
				Name:      "AAPL",
				Precision: 24 * time.Hour,
				Retention: 365 * 24 * time.Hour,
				Fields: []Field{
					{Name: "open", Aggregate: AggFirst},
					{Name: "close", Aggregate: AggLast},
					{Name: "volume", Aggregate: AggSum},
					{Name: "high", Aggregate: AggMax},
				},
			}); err != nil {
				t.Fatal(err)
			}

			// Now 3 of 4 defined fields resolve -> partial, for both reads.
			if _, cov, err := s.At(ctx, "AAPL", day); err != nil || cov != CoveragePartial {
				t.Fatalf("At after field add: cov=%v err=%v, want CoveragePartial", cov, err)
			}
			if _, cov, err := s.Latest(ctx, "AAPL"); err != nil || cov != CoveragePartial {
				t.Fatalf("Latest after field add: cov=%v err=%v, want CoveragePartial", cov, err)
			}

			// Writing the missing field restores full coverage.
			if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{"high": 110}}); err != nil {
				t.Fatal(err)
			}
			if _, cov, err := s.At(ctx, "AAPL", day); err != nil || cov != CoverageFull {
				t.Fatalf("At after backfill: cov=%v err=%v, want CoverageFull", cov, err)
			}
			if _, cov, err := s.Latest(ctx, "AAPL"); err != nil || cov != CoverageFull {
				t.Fatalf("Latest after backfill: cov=%v err=%v, want CoverageFull", cov, err)
			}
		})
	}
}

// TestCoverage_String checks the human-readable forms used in test/log output.
func TestCoverage_String(t *testing.T) {
	cases := map[Coverage]string{
		CoverageNone:    "none",
		CoveragePartial: "partial",
		CoverageFull:    "full",
		Coverage(99):    "Coverage(99)",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Coverage(%d).String() = %q, want %q", int(c), got, want)
		}
	}
}

func TestDeletes(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestDeletes"))
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

			// Delete one point (all its fields); a matching record reports deleted=true
			deleted, err := s.Delete(context.Background(), "AAPL", d2)
			if err != nil {
				t.Fatal(err)
			}
			if !deleted {
				t.Fatalf("Delete(d2) deleted = false, want true")
			}
			var c int64
			if err := s.db.Model(&dbRecord{}).Where("time = ?", unixMilli(d2)).Count(&c).Error; err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("after Delete(d2) count = %d, want 0", c)
			}

			// Deleting the now-empty timestamp again is a no-op: deleted=false, nil error
			deleted, err = s.Delete(context.Background(), "AAPL", d2)
			if err != nil {
				t.Fatal(err)
			}
			if deleted {
				t.Fatalf("re-Delete(d2) deleted = true, want false")
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
			s, err := New(connDB(t, tdb,"TestPerSeriesFieldIndep"))
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
