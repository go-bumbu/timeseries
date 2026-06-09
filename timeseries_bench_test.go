package timeseries

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	timingNumIngest = 3650
	timingNumRuns   = 10
)

func p90(durs []time.Duration) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durs))
	copy(sorted, durs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[(len(sorted)-1)*90/100]
}

func TestTiming(t *testing.T) {
	for _, tdb := range testdbs.DBs() {
		t.Run(tdb.DbType(), func(t *testing.T) {
			var ingestDurs, retrieveDurs []time.Duration
			for run := 0; run < timingNumRuns; run++ {
				s, err := New(tdb.ConnDbName("TestTiming_" + strconv.Itoa(run)))
				if err != nil {
					t.Fatal(err)
				}
				base := time.Now().Truncate(time.Hour).Add(-time.Duration(run) * 365 * 24 * time.Hour)
				if err := s.DefineSeries(Series{Name: "ts", Precision: time.Hour, Retention: 365 * 24 * time.Hour}); err != nil {
					t.Fatal(err)
				}
				if err := s.DefineField(Field{Name: "v", Aggregate: AggAvg}); err != nil {
					t.Fatal(err)
				}

				pts := make([]Point, timingNumIngest)
				for i := range pts {
					pts[i] = Point{Time: base.Add(time.Duration(i) * time.Minute), Values: map[string]float64{"v": float64(i)}}
				}
				t1 := time.Now()
				if err := s.WriteMany("ts", pts); err != nil {
					t.Fatal(err)
				}
				ingestDurs = append(ingestDurs, time.Since(t1))

				_ = s.Maintain(context.Background())

				t3 := time.Now()
				if _, err := s.FieldRange("ts", "v", base, base.Add(time.Duration(timingNumIngest)*time.Minute)); err != nil {
					t.Fatal(err)
				}
				retrieveDurs = append(retrieveDurs, time.Since(t3))
			}
			t.Logf("%s ingest_%d p90: %s", tdb.DbType(), timingNumIngest, p90(ingestDurs))
			t.Logf("%s retrieve p90: %s", tdb.DbType(), p90(retrieveDurs))
		})
	}
}

// TestStorageFootprint guards against rowid-table / text-time regressions on SQLite.
func TestStorageFootprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fp.db")
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DefineSeries(Series{Name: "fp", Precision: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.DefineField(Field{Name: "v", Aggregate: ""}); err != nil {
		t.Fatal(err)
	}

	const n = 50000
	pts := make([]Point, n)
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range pts {
		pts[i] = Point{Time: base.Add(time.Duration(i) * time.Minute), Values: map[string]float64{"v": float64(i)}}
	}
	if err := s.WriteMany("fp", pts); err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	if _, err := sqlDB.Exec("VACUUM"); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	bytesPerRow := float64(info.Size()) / float64(n)
	t.Logf("footprint: %d bytes / %d rows = %.1f bytes/row", info.Size(), n, bytesPerRow)
	// Clustered WITHOUT ROWID lands ~22-28 B/row incl. page overhead. A rowid table
	// with a composite-PK index is ~39 B/row; text timestamps push well past 50.
	// A ceiling of 35 catches both regressions while leaving headroom for the happy path.
	if bytesPerRow > 35 {
		t.Fatalf("bytes/row = %.1f, expected under 35 (rowid-table or text-time regression?)", bytesPerRow)
	}
}
