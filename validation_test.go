package timeseries

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

// nonUTC is a fixed-offset zone used to prove the API rejects non-UTC times.
var nonUTC = time.FixedZone("EST", -5*3600)

// TestWrite_RejectsNonUTC asserts every write/query/delete path rejects a
// non-UTC time argument (M2: the library is UTC-only and does no zone
// conversion). Zero-time sentinels remain valid as unbounded range bounds.
func TestWrite_RejectsNonUTC(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestRejectsNonUTC"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			bad := time.Date(2025, 1, 2, 0, 0, 0, 0, nonUTC)

			if err := s.Write(ctx, "AAPL", Point{Time: bad, Values: map[string]float64{"close": 1}}); err == nil {
				t.Fatal("Write with non-UTC time: want error, got nil")
			}
			if err := s.WriteMany(ctx, "AAPL", []Point{{Time: bad, Values: map[string]float64{"close": 1}}}); err == nil {
				t.Fatal("WriteMany with non-UTC time: want error, got nil")
			}
			if _, err := s.Range(ctx, "AAPL", bad, time.Time{}); err == nil {
				t.Fatal("Range with non-UTC start: want error, got nil")
			}
			if _, err := s.FieldRange(ctx, "AAPL", "close", time.Time{}, bad); err == nil {
				t.Fatal("FieldRange with non-UTC end: want error, got nil")
			}
			if err := s.DeleteRange(ctx, "AAPL", bad, time.Time{}); err == nil {
				t.Fatal("DeleteRange with non-UTC start: want error, got nil")
			}
			if _, _, err := s.FieldAt(ctx, "AAPL", "close", bad); err == nil {
				t.Fatal("FieldAt with non-UTC time: want error, got nil")
			}
			if _, _, err := s.At(ctx, "AAPL", bad); err == nil {
				t.Fatal("At with non-UTC time: want error, got nil")
			}
			if _, err := s.Delete(ctx, "AAPL", bad); err == nil {
				t.Fatal("Delete with non-UTC time: want error, got nil")
			}
		})
	}
}

// TestWrite_RejectsEmptyValues asserts a point with no field values is rejected
// rather than silently persisting nothing (M3).
func TestWrite_RejectsEmptyValues(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestRejectsEmptyValues"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

			if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{}}); err == nil {
				t.Fatal("Write with empty Values: want error, got nil")
			}
			if err := s.Write(ctx, "AAPL", Point{Time: day, Values: nil}); err == nil {
				t.Fatal("Write with nil Values: want error, got nil")
			}
			// Nothing should have been persisted.
			if n, _ := s.Count(ctx, "AAPL"); n != 0 {
				t.Fatalf("Count = %d after rejected empty writes, want 0", n)
			}
		})
	}
}

// TestWrite_RejectsNonFinite asserts NaN and ±Inf are rejected at the write
// boundary (M4) rather than silently corrupting (NaN -> NULL -> 0.0) or
// poisoning aggregates (+Inf).
func TestWrite_RejectsNonFinite(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestRejectsNonFinite"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

			for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
				if err := s.Write(ctx, "AAPL", Point{Time: day, Values: map[string]float64{"close": v}}); err == nil {
					t.Fatalf("Write with non-finite value %v: want error, got nil", v)
				}
			}
			if n, _ := s.Count(ctx, "AAPL"); n != 0 {
				t.Fatalf("Count = %d after rejected non-finite writes, want 0", n)
			}
		})
	}
}

// TestWriteMany_RejectsDuplicateKeyInBatch asserts a duplicate (time, field)
// within one batch is rejected (M5) instead of relying on dialect-specific
// upsert behavior (SQLite last-wins; PostgreSQL/MySQL error). Distinct fields
// at the same timestamp are NOT duplicates and must still be accepted.
func TestWriteMany_RejectsDuplicateKeyInBatch(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestRejectsDupKey"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

			// Same (time, field) twice -> rejected, nothing persisted.
			err = s.WriteMany(ctx, "AAPL", []Point{
				{Time: day, Values: map[string]float64{"close": 1}},
				{Time: day, Values: map[string]float64{"close": 2}},
			})
			if err == nil {
				t.Fatal("WriteMany with duplicate (time,field): want error, got nil")
			}
			if n, _ := s.Count(ctx, "AAPL"); n != 0 {
				t.Fatalf("Count = %d after rejected duplicate batch, want 0", n)
			}

			// Two distinct sub-millisecond instants collapse to the same stored key,
			// so they are duplicates too.
			t1 := time.Date(2025, 1, 3, 0, 0, 0, 500_000_000, time.UTC)
			t2 := t1.Add(300 * time.Microsecond)
			if err := s.WriteMany(ctx, "AAPL", []Point{
				{Time: t1, Values: map[string]float64{"close": 1}},
				{Time: t2, Values: map[string]float64{"close": 2}},
			}); err == nil {
				t.Fatal("WriteMany with sub-ms-distinct same-field points: want error, got nil")
			}

			// Distinct fields at the same timestamp are fine.
			if err := s.WriteMany(ctx, "AAPL", []Point{
				{Time: day, Values: map[string]float64{"close": 1}},
				{Time: day, Values: map[string]float64{"open": 2}},
			}); err != nil {
				t.Fatalf("WriteMany distinct fields same time: want nil, got %v", err)
			}
		})
	}
}

// TestWrite_MillisecondFloor pins the documented ms resolution (M1): two writes
// less than a millisecond apart resolve to the same stored key, so the second
// overwrites the first (separate Write calls, so no intra-batch duplicate check
// fires). The stored timestamp is floored to the millisecond.
func TestWrite_MillisecondFloor(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestMillisFloor"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			setupAAPL(t, s)
			floor := time.Date(2025, 1, 2, 0, 0, 0, 500_000_000, time.UTC) // .500s exactly
			sub := floor.Add(300 * time.Microsecond)                       // .500300s, same ms

			if err := s.Write(ctx, "AAPL", Point{Time: floor, Values: map[string]float64{"close": 11}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Write(ctx, "AAPL", Point{Time: sub, Values: map[string]float64{"close": 22}}); err != nil {
				t.Fatal(err)
			}

			if n, _ := s.Count(ctx, "AAPL"); n != 1 {
				t.Fatalf("Count = %d, want 1 (sub-ms writes collapse to one key)", n)
			}
			sm, found, err := s.LatestField(ctx, "AAPL", "close")
			if err != nil || !found {
				t.Fatalf("LatestField: found=%v err=%v", found, err)
			}
			if sm.Value != 22 {
				t.Fatalf("close = %v, want 22 (later sub-ms write wins)", sm.Value)
			}
			if !sm.Time.Equal(floor) {
				t.Fatalf("stored time = %v, want %v (floored to ms)", sm.Time, floor)
			}
		})
	}
}

// TestMaintain_RetentionBoundary pins the strict (<) retention cutoff (M6): a
// record exactly at now-retention is KEPT; one a millisecond older is purged.
// nowFunc is overridden so the cutoff is deterministic.
func TestMaintain_RetentionBoundary(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			s, err := New(connDB(t, tdb,"TestRetentionBoundary"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const retention = 48 * time.Hour
			if err := s.DefineSeries(ctx, Series{
				Name: "R", Precision: time.Hour, Retention: retention,
				Fields: []Field{{Name: "v", Aggregate: ""}},
			}); err != nil {
				t.Fatal(err)
			}

			now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
			restore := nowFunc
			nowFunc = func() time.Time { return now }
			defer func() { nowFunc = restore }()

			cutoff := now.Add(-retention)
			atCutoff := cutoff                         // exactly cutoff: kept (strict <)
			justOlder := cutoff.Add(-time.Millisecond) // one ms older: purged
			recent := now.Add(-time.Hour)              // well inside: kept
			if err := s.WriteMany(ctx, "R", []Point{
				{Time: justOlder, Values: map[string]float64{"v": 1}},
				{Time: atCutoff, Values: map[string]float64{"v": 2}},
				{Time: recent, Values: map[string]float64{"v": 3}},
			}); err != nil {
				t.Fatal(err)
			}

			if err := s.Maintain(ctx); err != nil {
				t.Fatal(err)
			}

			got, err := s.FieldRange(ctx, "R", "v", time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 {
				t.Fatalf("after retention: %d samples, want 2 (cutoff kept, older purged): %+v", len(got), got)
			}
			if !got[0].Time.Equal(atCutoff) {
				t.Fatalf("surviving oldest = %v, want exactly the cutoff %v (strict <)", got[0].Time, atCutoff)
			}
		})
	}
}
