package timeseries

import (
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

func TestDefineSeries_FieldSync(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestFieldSync"))
			if err != nil {
				t.Fatal(err)
			}
			day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			long := 100 * 365 * 24 * time.Hour

			// initial: fields a, b
			if err := s.DefineSeries(Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggLast}, {Name: "b", Aggregate: AggSum}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write("S", Point{Time: day, Values: map[string]float64{"a": 1, "b": 2}}); err != nil {
				t.Fatal(err)
			}

			// redefine: drop b, change a's aggregate, add c
			if err := s.DefineSeries(Series{
				Name: "S", Precision: 24 * time.Hour, Retention: long,
				Fields: []Field{{Name: "a", Aggregate: AggFirst}, {Name: "c", Aggregate: AggMax}},
			}); err != nil {
				t.Fatal(err)
			}

			got, err := s.GetSeries("S")
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Fields) != 2 {
				t.Fatalf("fields = %+v, want a and c", got.Fields)
			}
			names := map[string]bool{}
			for _, f := range got.Fields {
				names[f.Name] = true
			}
			if !names["a"] || !names["c"] {
				t.Fatalf("fields = %+v, want exactly a and c", got.Fields)
			}
			var aAgg string
			for _, f := range got.Fields {
				if f.Name == "a" {
					aAgg = f.Aggregate
				}
			}
			if aAgg != AggFirst {
				t.Fatalf("a aggregate = %q, want %q", aAgg, AggFirst)
			}

			// a's record survives; b is gone and rejected on write
			if _, found, _ := s.FieldAt("S", "a", day); !found {
				t.Fatal("field a record should survive the sync")
			}
			if err := s.Write("S", Point{Time: day, Values: map[string]float64{"b": 9}}); err == nil {
				t.Fatal("writing dropped field b should error")
			}
			var count int64
			s.db.Model(&dbRecord{}).Count(&count)
			if count != 1 {
				t.Fatalf("record count = %d, want 1 (b's record cascaded)", count)
			}
		})
	}
}

func TestDefineSeries_UnknownAggregate(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestFieldBadAgg"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.DefineSeries(Series{
		Name: "x", Precision: time.Hour, Retention: time.Hour,
		Fields: []Field{{Name: "v", Aggregate: "nope"}},
	})
	if err == nil {
		t.Fatal("expected error for unknown aggregate, got nil")
	}
}
