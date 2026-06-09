package timeseries

import (
	"context"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

func TestMaintain_Reduction(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMaintainReduce"))
			if err != nil {
				t.Fatal(err)
			}
			// daily precision; long retention so cleanup doesn't interfere
			if err := s.DefineSeries(Series{
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
			if err := s.WriteMany("X", []Point{
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
			v, found, err := s.FieldAt("X", "close", day.Add(24*time.Hour))
			if err != nil || !found || v != 105 {
				t.Fatalf("close after reduce = %v found=%v err=%v, want 105", v, found, err)
			}
			hi, _, _ := s.FieldAt("X", "high", day.Add(24*time.Hour))
			if hi != 110 {
				t.Fatalf("high after reduce = %v, want 110", hi)
			}

			closes, _ := s.FieldRange("X", "close", day, day.Add(24*time.Hour))
			if len(closes) != 1 || !closes[0].Time.Equal(day) {
				t.Fatalf("reduced close samples = %+v, want one at bucket start", closes)
			}

			// idempotent: second run changes nothing
			if err := s.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			closes2, _ := s.FieldRange("X", "close", day, day.Add(24*time.Hour))
			if len(closes2) != 1 {
				t.Fatalf("Maintain not idempotent: %+v", closes2)
			}
		})
	}
}

func TestMaintain_AfterFieldDrop(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMaintainAfterDrop"))
			if err != nil {
				t.Fatal(err)
			}
			long := 100 * 365 * 24 * time.Hour
			// series with fields a and b
			if err := s.DefineSeries(Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggMax}, {Name: "b", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}
			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			// two sub-bucket points for both fields
			if err := s.WriteMany("S", []Point{
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"a": 1, "b": 5}},
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"a": 2, "b": 6}},
			}); err != nil {
				t.Fatal(err)
			}

			// drop b (cascades its records), keep a
			if err := s.DefineSeries(Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(context.Background()); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			// a reduced to its max, collapsed to one row at bucket start
			as, err := s.FieldRange("S", "a", day, day.Add(24*time.Hour))
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
			s, err := New(tdb.ConnDbName("TestMaintainNoAgg"))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.DefineSeries(Series{
				Name: "N", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour,
				Fields: []Field{{Name: "raw", Aggregate: ""}},
			}); err != nil {
				t.Fatal(err)
			}
			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			// two sub-bucket points in the same day
			if err := s.WriteMany("N", []Point{
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"raw": 1}},
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"raw": 2}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			// empty aggregate => both raw rows kept (no collapse)
			samples, err := s.FieldRange("N", "raw", day, day.Add(24*time.Hour))
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
	s, err := New(testdbs.DBs()[0].ConnDbName("TestMaintainRetention"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DefineSeries(Series{
		Name: "R", Precision: 24 * time.Hour, Retention: 48 * time.Hour,
		Fields: []Field{{Name: "v", Aggregate: ""}},
	}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-10 * 24 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)
	if err := s.WriteMany("R", []Point{
		{Time: old, Values: map[string]float64{"v": 1}},
		{Time: recent, Values: map[string]float64{"v": 2}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	var c int64
	s.db.Model(&dbRecord{}).Count(&c)
	if c != 1 {
		t.Fatalf("after retention count = %d, want 1 (old purged)", c)
	}
}

func TestMaintain_MultiFieldSinglePass(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMaintainMultiField"))
			if err != nil {
				t.Fatal(err)
			}
			long := 100 * 365 * 24 * time.Hour
			if err := s.DefineSeries(Series{
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
			if err := s.WriteMany("X", []Point{
				{Time: day.Add(16 * time.Hour), Values: map[string]float64{"close": 105, "high": 110, "raw": 1}},
				{Time: day.Add(9 * time.Hour), Values: map[string]float64{"close": 101, "high": 103, "raw": 2}},
				{Time: day.Add(12 * time.Hour), Values: map[string]float64{"close": 102, "high": 108, "raw": 3}},
			}); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(context.Background()); err != nil {
				t.Fatalf("Maintain: %v", err)
			}

			if v, found, err := s.FieldAt("X", "close", day.Add(24*time.Hour)); err != nil || !found || v != 105 {
				t.Fatalf("close = %v found=%v err=%v, want 105 (last)", v, found, err)
			}
			if v, found, err := s.FieldAt("X", "high", day.Add(24*time.Hour)); err != nil || !found || v != 110 {
				t.Fatalf("high = %v found=%v err=%v, want 110 (max)", v, found, err)
			}
			// raw has no aggregate -> all three rows kept
			raws, err := s.FieldRange("X", "raw", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(raws) != 3 {
				t.Fatalf("raw rows = %d, want 3 (no reduction)", len(raws))
			}
			// reduced fields collapsed to one row at bucket start
			closes, err := s.FieldRange("X", "close", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(closes) != 1 || !closes[0].Time.Equal(day) {
				t.Fatalf("close reduced = %+v, want one at bucket start", closes)
			}
			highs, err := s.FieldRange("X", "high", day, day.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(highs) != 1 || !highs[0].Time.Equal(day) {
				t.Fatalf("high reduced = %+v, want one at bucket start", highs)
			}

			// idempotent
			if err := s.Maintain(context.Background()); err != nil {
				t.Fatal(err)
			}
			closes2, _ := s.FieldRange("X", "close", day, day.Add(24*time.Hour))
			if len(closes2) != 1 {
				t.Fatalf("not idempotent: %+v", closes2)
			}
		})
	}
}
