package timeseries

import (
	"testing"

	"github.com/go-bumbu/testdbs"
)

func TestDefineField(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(tdb.ConnDbName("TestDefineField"))
			if err != nil {
				t.Fatal(err)
			}

			if err := s.DefineField(Field{Name: "close", Aggregate: AggLast}); err != nil {
				t.Fatalf("DefineField: %v", err)
			}
			// update in place
			if err := s.DefineField(Field{Name: "close", Aggregate: AggFirst}); err != nil {
				t.Fatalf("DefineField update: %v", err)
			}

			fields, err := s.ListFields()
			if err != nil {
				t.Fatal(err)
			}
			if len(fields) != 1 || fields[0].Name != "close" || fields[0].Aggregate != AggFirst {
				t.Fatalf("ListFields = %+v, want one close/first", fields)
			}
		})
	}
}

func TestDefineField_UnknownAggregate(t *testing.T) {
	s, err := New(testdbs.DBs()[0].ConnDbName("TestDefineFieldBad"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DefineField(Field{Name: "x", Aggregate: "nope"}); err == nil {
		t.Fatal("expected error for unknown aggregate, got nil")
	}
}
