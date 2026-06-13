package timeseries

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestMaintain_Reduction(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainReduce"))
			if err != nil {
				t.Fatal(err)
			}
			// daily precision; long retention so cleanup doesn't interfere
			if err := s.DefineSeries(context.Background(), Series{
				Name: "X", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour,
				Fields: []Field{
					{Name: "close", Aggregate: AggLast},
					{Name: "high", Aggregate: AggMax},
				},
			}); err != nil {
				t.Fatal(err)
			}

			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			// three sub-bucket points on the same day, written out of order
			if err := s.WriteMany(context.Background(), "X", []Point{
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"close": 105, "high": 110}},
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"close": 101, "high": 103}},
				{Time: day.Add(12 * time.Hour), Values: map[string]float64{"close": 102, "high": 108}},
			}); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(context.Background()); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			// close=last -> 105 (latest time); high=max -> 110; collapsed to bucket start
			v, found, err := s.FieldAt(context.Background(), "X", "close", day.Add(24*time.Hour))
			if err != nil || !found || v != 105 {
				t.Fatalf("close after reduce = %v found=%v err=%v, want 105", v, found, err)
			}
			hi, _, _ := s.FieldAt(context.Background(), "X", "high", day.Add(24*time.Hour))
			if hi != 110 {
				t.Fatalf("high after reduce = %v, want 110", hi)
			}

			closes, _ := s.FieldRange(context.Background(), "X", "close", day, day.Add(24*time.Hour))
			if len(closes) != 1 || !closes[0].Time.Equal(day) {
				t.Fatalf("reduced close samples = %+v, want one at bucket start", closes)
			}

			// idempotent: second run changes nothing
			if err := s.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			closes2, _ := s.FieldRange(context.Background(), "X", "close", day, day.Add(24*time.Hour))
			if len(closes2) != 1 {
				t.Fatalf("Maintain not idempotent: %+v", closes2)
			}
		})
	}
}

func TestMaintain_AfterFieldDrop(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainAfterDrop"))
			if err != nil {
				t.Fatal(err)
			}
			long := 100 * 365 * 24 * time.Hour
			// series with fields a and b
			if err := s.DefineSeries(context.Background(), Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggMax}, {Name: "b", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}
			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			// two sub-bucket points for both fields
			if err := s.WriteMany(context.Background(), "S", []Point{
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"a": 1, "b": 5}},
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"a": 2, "b": 6}},
			}); err != nil {
				t.Fatal(err)
			}

			// drop b (cascades its records), keep a
			if err := s.DefineSeries(context.Background(), Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(context.Background()); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			// a reduced to its max, collapsed to one row at bucket start
			as, err := s.FieldRange(context.Background(), "S", "a", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(as) != 1 || as[0].Value != 2 || !as[0].Time.Equal(day) {
				t.Fatalf("a after reduce = %+v, want one row value 2 at bucket start", as)
			}
			// b's records are gone entirely; only the single reduced a row remains
			var total int64
			if err := s.db.Model(&dbRecord{}).Count(&total).Error; err != nil {
				t.Fatal(err)
			}
			if total != 1 {
				t.Fatalf("record count = %d, want 1 (only reduced a; b cascaded)", total)
			}
		})
	}
}

func TestMaintain_NoAggregateNoReduction(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainNoAgg"))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.DefineSeries(context.Background(), Series{
				Name: "N", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour,
				Fields: []Field{{Name: "raw", Aggregate: ""}},
			}); err != nil {
				t.Fatal(err)
			}
			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			// two sub-bucket points in the same day
			if err := s.WriteMany(context.Background(), "N", []Point{
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"raw": 1}},
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"raw": 2}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			// empty aggregate => both raw rows kept (no collapse)
			samples, err := s.FieldRange(context.Background(), "N", "raw", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(samples) != 2 {
				t.Fatalf("empty-aggregate field reduced to %d rows, want 2 (no reduction)", len(samples))
			}
		})
	}
}

func TestMaintain_Retention(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainRetention"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := s.DefineSeries(ctx, Series{
				Name: "R", Precision: 24 * time.Hour, Retention: 48 * time.Hour,
				Fields: []Field{{Name: "v", Aggregate: ""}},
			}); err != nil {
				t.Fatal(err)
			}
			old := time.Now().UTC().Add(-10 * 24 * time.Hour)
			recent := time.Now().UTC().Add(-1 * time.Hour)
			if err := s.WriteMany(ctx, "R", []Point{
				{Time: old, Values: map[string]float64{"v": 1}},
				{Time: recent, Values: map[string]float64{"v": 2}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Maintain(ctx); err != nil {
				t.Fatal(err)
			}
			var c int64
			if err := s.db.Model(&dbRecord{}).Count(&c).Error; err != nil {
				t.Fatal(err)
			}
			if c != 1 {
				t.Fatalf("after retention count = %d, want 1 (old purged)", c)
			}
		})
	}
}

func TestMaintain_MultiFieldSinglePass(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainMultiField"))
			if err != nil {
				t.Fatal(err)
			}
			runMaintainMultiField(t, s)
		})
	}
}

func runMaintainMultiField(t *testing.T, s *Store) {
	t.Helper()
	long := 100 * 365 * 24 * time.Hour
	if err := s.DefineSeries(context.Background(), Series{
		Name: "X", Precision: 24 * time.Hour, Retention: long,
		Fields: []Field{
			{Name: "close", Aggregate: AggLast},
			{Name: "high", Aggregate: AggMax},
			{Name: "raw", Aggregate: ""}, // no reduction
		},
	}); err != nil {
		t.Fatal(err)
	}
	day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s.WriteMany(context.Background(), "X", []Point{
		{Time: day.Add(16 * time.Hour), Values: map[string]float64{"close": 105, "high": 110, "raw": 1}},
		{Time: day.Add(9 * time.Hour), Values: map[string]float64{"close": 101, "high": 103, "raw": 2}},
		{Time: day.Add(12 * time.Hour), Values: map[string]float64{"close": 102, "high": 108, "raw": 3}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.Maintain(context.Background()); err != nil {
		t.Fatalf("Maintain: %v", err)
	}

	if v, found, err := s.FieldAt(context.Background(), "X", "close", day.Add(24*time.Hour)); err != nil || !found || v != 105 {
		t.Fatalf("close = %v found=%v err=%v, want 105 (last)", v, found, err)
	}
	if v, found, err := s.FieldAt(context.Background(), "X", "high", day.Add(24*time.Hour)); err != nil || !found || v != 110 {
		t.Fatalf("high = %v found=%v err=%v, want 110 (max)", v, found, err)
	}
	raws, err := s.FieldRange(context.Background(), "X", "raw", day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(raws) != 3 {
		t.Fatalf("raw rows = %d, want 3 (no reduction)", len(raws))
	}
	closes, err := s.FieldRange(context.Background(), "X", "close", day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(closes) != 1 || !closes[0].Time.Equal(day) {
		t.Fatalf("close reduced = %+v, want one at bucket start", closes)
	}
	highs, err := s.FieldRange(context.Background(), "X", "high", day, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(highs) != 1 || !highs[0].Time.Equal(day) {
		t.Fatalf("high reduced = %+v, want one at bucket start", highs)
	}
	if err := s.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	closes2, _ := s.FieldRange(context.Background(), "X", "close", day, day.Add(24*time.Hour))
	if len(closes2) != 1 {
		t.Fatalf("not idempotent: %+v", closes2)
	}
}

// TestMaintain_ReductionAcrossChunkBoundary drives reduceSeries past its
// reduceChunkBuckets chunk size so the chunked loop is exercised for real: a
// bucket must never straddle a chunk edge, and the ascending-time aggregate
// contract (first = earliest sample, last = latest) must hold across boundaries.
// Each hour holds three sub-bucket points written out of time order; after
// Maintain every bucket must collapse to exactly one row per field at the bucket
// start with the right first/last values.
func TestMaintain_ReductionAcrossChunkBoundary(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestReduceChunkBoundary"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const long = 100 * 365 * 24 * time.Hour
			if err := s.DefineSeries(ctx, Series{
				Name: "X", Precision: time.Hour, Retention: long,
				Fields: []Field{{Name: "first", Aggregate: AggFirst}, {Name: "last", Aggregate: AggLast}},
			}); err != nil {
				t.Fatal(err)
			}

			const buckets = 2*reduceChunkBuckets + 50 // 250: crosses two chunk edges
			base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			// value encodes (bucketIndex, minute) so first/last are computable:
			// first must resolve to the :10 value, last to the :50 value, in every
			// bucket, regardless of insertion order.
			val := func(b, min int) float64 { return float64(b*100 + min) }
			var pts []Point
			for b := 0; b < buckets; b++ {
				bucket := base.Add(time.Duration(b) * time.Hour)
				for _, min := range []int{30, 50, 10} { // out of time order on purpose
					pts = append(pts, Point{
						Time:   bucket.Add(time.Duration(min) * time.Minute),
						Values: map[string]float64{"first": val(b, min), "last": val(b, min)},
					})
				}
			}
			if err := s.WriteMany(ctx, "X", pts); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(ctx); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			// Each bucket -> one row per field at the bucket start. Two fields.
			var total int64
			if err := s.db.Model(&dbRecord{}).Count(&total).Error; err != nil {
				t.Fatal(err)
			}
			if want := int64(buckets * 2); total != want {
				t.Fatalf("post-reduce row count = %d, want %d (one row per field per bucket)", total, want)
			}

			// Spot-check the buckets straddling each chunk boundary plus the ends.
			for _, b := range []int{0, reduceChunkBuckets - 1, reduceChunkBuckets, reduceChunkBuckets + 1, 2 * reduceChunkBuckets, buckets - 1} {
				bucket := base.Add(time.Duration(b) * time.Hour)
				firsts, err := s.FieldRange(ctx, "X", "first", bucket, bucket)
				if err != nil {
					t.Fatal(err)
				}
				if len(firsts) != 1 || firsts[0].Value != val(b, 10) || !firsts[0].Time.Equal(bucket) {
					t.Fatalf("bucket %d first = %+v, want one sample %v at %v", b, firsts, val(b, 10), bucket)
				}
				lasts, err := s.FieldRange(ctx, "X", "last", bucket, bucket)
				if err != nil {
					t.Fatal(err)
				}
				if len(lasts) != 1 || lasts[0].Value != val(b, 50) || !lasts[0].Time.Equal(bucket) {
					t.Fatalf("bucket %d last = %+v, want one sample %v at %v", b, lasts, val(b, 50), bucket)
				}
			}
		})
	}
}

// TestMaintain_ContinuesAfterPerSeriesError verifies Maintain's error policy:
// a per-series failure is collected (errors.Join) and the sweep continues to the
// other series rather than aborting. A GORM delete callback injects a DB error for
// every records delete targeting the "BAD" series (series_id is the first WHERE arg
// in every records delete in Maintain), leaving "GOOD" untouched. Maintain must
// return a non-nil error AND still reduce GOOD; BAD's reduction must roll back.
func TestMaintain_ContinuesAfterPerSeriesError(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMaintainPartialFailure"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const long = 100 * 365 * 24 * time.Hour
			mk := func(name string) {
				if err := s.DefineSeries(ctx, Series{
					Name: name, Precision: 24 * time.Hour, Retention: long,
					Fields: []Field{{Name: "v", Aggregate: AggMax}},
				}); err != nil {
					t.Fatalf("DefineSeries %s: %v", name, err)
				}
			}
			mk("GOOD")
			mk("BAD")

			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			for _, name := range []string{"GOOD", "BAD"} {
				if err := s.WriteMany(ctx, name, []Point{
					{Time: day.Add(9 * time.Hour), Values: map[string]float64{"v": 1}},
					{Time: day.Add(16 * time.Hour), Values: map[string]float64{"v": 2}},
				}); err != nil {
					t.Fatal(err)
				}
			}

			badID, err := s.seriesID(ctx, "BAD")
			if err != nil {
				t.Fatal(err)
			}

			// Fail every records delete whose series_id is BAD (the first WHERE arg
			// in every records delete Maintain issues), leaving GOOD untouched.
			injected := errors.New("injected failure for BAD")
			removeInjector := failRecordDeletesForSeries(t, s.db, badID, injected)

			mErr := s.Maintain(ctx)

			// Remove the injector before any read-back so the read path is clean.
			removeInjector()

			if mErr == nil {
				t.Fatal("Maintain returned nil, want the injected BAD error (failures must be collected, not swallowed)")
			}
			if !errors.Is(mErr, injected) {
				t.Fatalf("Maintain err = %v, want it to wrap the injected BAD error", mErr)
			}

			// GOOD reduced despite BAD failing: its two sub-bucket points collapsed
			// to one row (max=2) at the bucket start.
			goodRows, err := s.FieldRange(ctx, "GOOD", "v", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(goodRows) != 1 || goodRows[0].Value != 2 || !goodRows[0].Time.Equal(day) {
				t.Fatalf("GOOD after Maintain = %+v, want one reduced row value 2 at bucket start (Maintain must continue past BAD)", goodRows)
			}

			// BAD's reduction rolled back: its two raw points are still present.
			badRows, err := s.FieldRange(ctx, "BAD", "v", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(badRows) != 2 {
				t.Fatalf("BAD rows = %d, want 2 (its reduce failed and rolled back)", len(badRows))
			}
		})
	}
}

// failRecordDeletesForSeries registers a GORM Before-delete callback that injects
// err into every records-table delete whose first WHERE arg (series_id) equals
// seriesID. At the Before-delete hook the bound Vars aren't built yet, but the
// .Where() args live in the WHERE clause expression. It returns a func that
// removes the callback again.
func failRecordDeletesForSeries(t *testing.T, db *gorm.DB, seriesID uint, injected error) func() {
	t.Helper()
	const cb = "inject_bad_failure"
	if err := db.Callback().Delete().Before("gorm:delete").Register(cb, func(d *gorm.DB) {
		if d.Statement.Table != (dbRecord{}).TableName() {
			return
		}
		where, ok := d.Statement.Clauses["WHERE"].Expression.(clause.Where)
		if !ok || len(where.Exprs) == 0 {
			return
		}
		expr, ok := where.Exprs[0].(clause.Expr)
		if !ok || len(expr.Vars) == 0 {
			return
		}
		if id, ok := expr.Vars[0].(uint); ok && id == seriesID {
			_ = d.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	return func() { _ = db.Callback().Delete().Remove(cb) }
}
