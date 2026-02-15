package timeseries

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
)

const (
	timingNumIngest   = 3650
	timingNumRetrieve = 10
	timingNumRuns     = 10
)

// p90 returns the 90th percentile duration from the slice (sorted ascending).
func p90(durs []time.Duration) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durs))
	copy(sorted, durs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (len(sorted) - 1) * 90 / 100
	return sorted[idx]
}

// TestTiming runs the full sequence (register, ingest 3650, maintenance, retrieve 10x) timingNumRuns times
// per DB, then logs the 90th percentile duration for each operation.
func TestTiming(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			var registerDurs, ingestDurs, maintenanceDurs, retrieveDurs []time.Duration

			for run := 0; run < timingNumRuns; run++ {
				dbCon := db.ConnDbName("TestTiming_" + strconv.Itoa(run))
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}

				seriesName := "timing_series"
				base := time.Now().Truncate(time.Hour).Add(-time.Duration(run) * 365 * 24 * time.Hour)

				// 1. Register series
				t0 := time.Now()
				series := TimeSeries{
					Name: seriesName,
					Retention: SamplingPolicy{
						Precision:   time.Hour,
						Retention:   365 * 24 * time.Hour,
						AggregateFn: AggregateAVG,
					},
				}
				if err := store.RegisterSeries(series); err != nil {
					t.Fatalf("RegisterSeries: %v", err)
				}
				registerDurs = append(registerDurs, time.Since(t0))

				// 2. Ingest 3650 items in bulk
				points := make([]DataPoint, timingNumIngest)
				for i := 0; i < timingNumIngest; i++ {
					points[i] = DataPoint{
						Time:  base.Add(time.Duration(i) * time.Minute),
						Value: float64(i),
					}
				}
				t1 := time.Now()
				if _, err := store.IngestBulk(seriesName, points); err != nil {
					t.Fatalf("IngestBulk(%d): %v", timingNumIngest, err)
				}
				ingestDurs = append(ingestDurs, time.Since(t1))

				// 3. Run maintenance once
				t2 := time.Now()
				if err := store.Maintenance(context.Background()); err != nil {
					t.Fatalf("Maintenance: %v", err)
				}
				maintenanceDurs = append(maintenanceDurs, time.Since(t2))

				// 4. Retrieve 10 times (ListRecords over full range)
				start := base
				end := base.Add(time.Duration(timingNumIngest) * time.Minute)
				t3 := time.Now()
				for i := 0; i < timingNumRetrieve; i++ {
					if _, err := store.ListRecords(seriesName, start, end); err != nil {
						t.Fatalf("ListRecords: %v", err)
					}
				}
				retrieveDurs = append(retrieveDurs, time.Since(t3))
			}

			t.Logf("%s register_series p90: %s (n=%d)", db.DbType(), p90(registerDurs), timingNumRuns)
			t.Logf("%s ingest_bulk_%d p90: %s (n=%d)", db.DbType(), timingNumIngest, p90(ingestDurs), timingNumRuns)
			t.Logf("%s maintenance p90: %s (n=%d)", db.DbType(), p90(maintenanceDurs), timingNumRuns)
			t.Logf("%s retrieve_x%d p90: %s (n=%d)", db.DbType(), timingNumRetrieve, p90(retrieveDurs), timingNumRuns)
		})
	}
}
