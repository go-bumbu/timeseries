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
			if err := s.DefineSeries(Series{Name: "X", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour}); err != nil {
				t.Fatal(err)
			}
			if err := s.DefineField(Field{Name: "close", Aggregate: AggLast}); err != nil {
				t.Fatal(err)
			}
			if err := s.DefineField(Field{Name: "high", Aggregate: AggMax}); err != nil {
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

func TestMaintain_NoAggregateNoReduction(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestMaintainNoAgg"))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.DefineSeries(Series{Name: "N", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour}); err != nil {
				t.Fatal(err)
			}
			if err := s.DefineField(Field{Name: "raw", Aggregate: ""}); err != nil {
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
	if err := s.DefineSeries(Series{Name: "R", Precision: 24 * time.Hour, Retention: 48 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.DefineField(Field{Name: "v", Aggregate: ""}); err != nil {
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
