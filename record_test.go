package timeseries

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var testRecords = []Record{
	{Series: "btc_price", Time: getDateTime("2025-01-01 00:00:00"), Value: 10000},
	{Series: "btc_price", Time: getDateTime("2025-01-02 00:00:00"), Value: 11000},
	{Series: "btc_price", Time: getDateTime("2025-01-03 00:00:00"), Value: 12000},
	{Series: "btc_price", Time: getDateTime("2025-01-04 00:00:00"), Value: 13000},
	{Series: "btc_price", Time: getDateTime("2025-01-05 00:00:00"), Value: 14000},
	{Series: "btc_price", Time: getDateTime("2025-01-06 00:00:00"), Value: 15000},
	{Series: "btc_price", Time: getDateTime("2025-01-07 00:00:00"), Value: 16000},
	{Series: "btc_price", Time: getDateTime("2025-01-08 00:00:00"), Value: 17000},
	{Series: "btc_price", Time: getDateTime("2025-01-09 00:00:00"), Value: 18000},
	{Series: "btc_price", Time: getDateTime("2025-01-10 00:00:00"), Value: 19000},
}

func TestIngestSeries(t *testing.T) {
	tcs := []struct {
		name    string
		input   Record
		wantErr string
	}{
		{
			name: "create valid record",
			input: Record{
				Series: "btc_price",
				Time:   time.Now(),
				Value:  68000.5,
			},
		},
		{
			name: "want error on empty series name",
			input: Record{
				Time:  time.Now(),
				Value: 123.4,
			},
			wantErr: "timeseries name cannot be empty",
		},
		{
			name: "want error on zero time",
			input: Record{
				Series: "btc_price",
				Value:  100.0,
			},
			wantErr: "time value cannot be zero",
		},
		{
			name: "want error on missing series in getDb",
			input: Record{
				Series: "unknown_series",
				Time:   time.Now(),
				Value:  99.9,
			},
			wantErr: "failed to lookup series: record not found",
		},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {

			dbCon := db.ConnDbName("TestSeriesIngest")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			// Arrange: create a known series so that valid tests can use it
			existingSeries := dbTimeSeries{Name: "btc_price"}
			if err := store.db.Create(&existingSeries).Error; err != nil {
				t.Fatalf("failed to create test series: %v", err)
			}

			// Create main retention policy for the series
			mainPolicy := dbSamplingPolicy{
				TimeSeriesID:  existingSeries.ID,
				Name:          mainPolicyName,
				Precision:     time.Minute,
				Retention:     24 * time.Hour,
				AggregationFn: "avg",
			}
			if err := store.db.Create(&mainPolicy).Error; err != nil {
				t.Fatalf("failed to create main policy: %v", err)
			}
			existingSeries.Policies = []dbSamplingPolicy{mainPolicy}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {

					_, err := store.Ingest(tc.input.Series, tc.input.Time, tc.input.Value)

					if tc.wantErr != "" {
						// Expecting an error
						if err == nil {
							t.Fatalf("expected error: %s, but got none", tc.wantErr)
						}
						if !strings.Contains(err.Error(), tc.wantErr) {
							t.Errorf("expected error containing %q, got %v", tc.wantErr, err.Error())
						}

					} else {
						// No error expected
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}

						// Verify the record is in the DB
						var got []dbRecord
						if err := store.db.Find(&got).Error; err != nil {
							t.Fatalf("failed to query getDb: %v", err)
						}

						if len(got) == 0 {
							t.Fatalf("expected at least 1 record, got 0")
						}

						// Find the last inserted record
						sort.Slice(got, func(i, j int) bool {
							return got[i].Id > got[j].Id
						})

						last := got[0]

						if last.SamplingId != existingSeries.mainPolicyID() {
							t.Errorf("expected SamplingId=%d, got %d", existingSeries.mainPolicyID(), last.SamplingId)
						}

						if last.Value != tc.input.Value {
							t.Errorf("expected Value=%.2f, got %.2f", tc.input.Value, last.Value)
						}
					}

				})
			}
		})
	}
}

func TestIngestBulk(t *testing.T) {
	tcs := []struct {
		name      string
		series    string
		points    []DataPoint
		wantIDs   int
		wantErr   string
		skipSetup bool // true for "unknown series" so we don't register it
	}{
		{
			name:    "empty points returns nil ids",
			series:  "bulk_empty",
			points:  nil,
			wantIDs: 0,
		},
		{
			name:    "empty slice returns nil ids",
			series:  "bulk_empty_slice",
			points:  []DataPoint{},
			wantIDs: 0,
		},
		{
			name:    "single point",
			series:  "bulk_single",
			points:  []DataPoint{{Time: getDateTime("2025-01-01 12:00:00"), Value: 42}},
			wantIDs: 1,
		},
		{
			name:   "multiple points",
			series: "bulk_multi",
			points: []DataPoint{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 1},
				{Time: getDateTime("2025-01-01 11:00:00"), Value: 2},
				{Time: getDateTime("2025-01-01 12:00:00"), Value: 3},
			},
			wantIDs: 3,
		},
		{
			name:      "invalid series",
			series:    "unknown_series",
			points:    []DataPoint{{Time: getDateTime("2025-01-01 12:00:00"), Value: 1}},
			wantErr:   "failed to lookup series",
			skipSetup: true,
		},
		{
			name:      "empty series name",
			series:    "",
			points:    []DataPoint{{Time: getDateTime("2025-01-01 12:00:00"), Value: 1}},
			wantErr:   "timeseries name cannot be empty",
			skipSetup: true,
		},
		{
			name:      "zero time at index 0",
			series:    "bulk_zero",
			points:    []DataPoint{{Time: time.Time{}, Value: 1}},
			wantErr:   "time value cannot be zero",
			skipSetup: true,
		},
		{
			name:   "zero time at index 1",
			series: "bulk_zero_idx",
			points: []DataPoint{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 1},
				{Time: time.Time{}, Value: 2},
			},
			wantErr:   "time value cannot be zero",
			skipSetup: true,
		},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestIngestBulk")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					if !tc.skipSetup {
						ts := TimeSeries{
							Name: tc.series,
							Retention: SamplingPolicy{
								Precision: time.Minute,
								Retention: 24 * time.Hour,
							},
						}
						if err := store.RegisterSeries(ts); err != nil {
							t.Fatalf("setup: RegisterSeries: %v", err)
						}
					}

					ids, err := store.IngestBulk(tc.series, tc.points)

					if tc.wantErr != "" {
						if err == nil {
							t.Fatalf("expected error containing %q, got nil", tc.wantErr)
						}
						if !strings.Contains(err.Error(), tc.wantErr) {
							t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if tc.wantIDs == 0 {
						if ids != nil {
							t.Errorf("expected nil ids, got len=%d", len(ids))
						}
						return
					}
					if len(ids) != tc.wantIDs {
						t.Errorf("expected %d ids, got %d: %v", tc.wantIDs, len(ids), ids)
					}
					// IDs should be distinct and non-zero
					seen := make(map[uint]bool)
					for _, id := range ids {
						if id == 0 {
							t.Error("expected non-zero id")
						}
						if seen[id] {
							t.Errorf("duplicate id %d", id)
						}
						seen[id] = true
					}

					// Verify records in DB when we inserted something
					if tc.wantIDs > 0 && !tc.skipSetup {
						got, err := store.ListRecords(tc.series, time.Time{}, time.Time{})
						if err != nil {
							t.Fatalf("ListRecords: %v", err)
						}
						if len(got) != tc.wantIDs {
							t.Errorf("ListRecords: expected %d records, got %d", tc.wantIDs, len(got))
						}
					}
				})
			}
		})
	}
}

func TestUpdateRecord(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tcs := []struct {
			name       string
			policyName string // which policy the record belongs to (main only)
			update     RecordUpdate
			seriesName string
			wantValue  float64 // expected value after update
			wantTime   time.Time
		}{
			{
				name:       "update value only in main policy",
				policyName: "main",
				update:     RecordUpdate{Value: ptr(999.99)},
				seriesName: "btc_price",
				wantValue:  999.99,
				wantTime:   getDateTime("2022-01-07 00:00:00"),
			},
			{
				name:       "update time only in main policy",
				policyName: "main",
				update:     RecordUpdate{Time: ptr(getDateTime("2022-01-08 00:00:00"))},
				seriesName: "eth_price",
				wantValue:  100.0,
				wantTime:   getDateTime("2022-01-08 00:00:00"),
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				dbCon := db.ConnDbName("TestUpdateRecord_happy")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}

				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						series := dbTimeSeries{Name: tc.seriesName}
						if err := store.db.Create(&series).Error; err != nil {
							t.Fatalf("failed to create series: %v", err)
						}

						// Create main policy
						mainPolicy := dbSamplingPolicy{
							TimeSeriesID:  series.ID,
							Name:          "main",
							Precision:     time.Minute,
							Retention:     24 * time.Hour,
							AggregationFn: "avg",
						}
						if err := store.db.Create(&mainPolicy).Error; err != nil {
							t.Fatalf("failed to create main policy: %v", err)
						}

						targetPolicy := mainPolicy

						// Create record in target policy
						r1 := dbRecord{
							SamplingId: targetPolicy.ID,
							Time:       getDateTime("2022-01-07 00:00:00"),
							Value:      100.0,
						}
						if err := store.db.Create(&r1).Error; err != nil {
							t.Fatalf("failed to create record: %v", err)
						}

						// Update the record
						err := store.UpdateRecord(r1.Id, tc.update)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}

						// Verify by reading the record directly from DB
						var updated dbRecord
						if err := store.db.First(&updated, r1.Id).Error; err != nil {
							t.Fatalf("failed to read updated record: %v", err)
						}

						if updated.Value != tc.wantValue {
							t.Errorf("expected value %v, got %v", tc.wantValue, updated.Value)
						}
						if !updated.Time.Equal(tc.wantTime) {
							t.Errorf("expected time %v, got %v", tc.wantTime, updated.Time)
						}
					})
				}
			})
		}
	})

	t.Run("error cases", func(t *testing.T) {
		tcs := []struct {
			name     string
			targetID uint
			update   RecordUpdate
			wantErr  string
		}{
			{
				name:     "error on zero id",
				targetID: 0,
				wantErr:  "record id is required for update",
			},
			{
				name:     "error on non-existing record",
				targetID: 999,
				wantErr:  "record not found: record not found",
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				dbCon := db.ConnDbName("TestUpdateRecord_error")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}

				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						err := store.UpdateRecord(tc.targetID, tc.update)

						if err == nil {
							t.Fatalf("expected error %q, got none", tc.wantErr)
						}
						if diff := cmp.Diff(tc.wantErr, err.Error()); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					})
				}
			})
		}
	})
}

func TestDeleteRecord(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tcs := []struct {
			name       string
			policyName string
			seriesName string
		}{
			{
				name:       "delete record from main policy",
				policyName: "main",
				seriesName: "btc_price",
			},
			{
				name:       "delete non-existing record does not error",
				policyName: "main",
				seriesName: "ada_price",
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				dbCon := db.ConnDbName("TestDeleteRecord_happy")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}

				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						series := dbTimeSeries{Name: tc.seriesName}
						if err := store.db.Create(&series).Error; err != nil {
							t.Fatalf("failed to create series: %v", err)
						}

						// Create main policy
						mainPolicy := dbSamplingPolicy{
							TimeSeriesID:  series.ID,
							Name:          "main",
							Precision:     time.Minute,
							Retention:     24 * time.Hour,
							AggregationFn: "avg",
						}
						if err := store.db.Create(&mainPolicy).Error; err != nil {
							t.Fatalf("failed to create main policy: %v", err)
						}

						targetPolicy := mainPolicy

						// Create record in target policy
						r1 := dbRecord{
							SamplingId: targetPolicy.ID,
							Time:       getDateTime("2022-01-07 00:00:00"),
							Value:      100.0,
						}
						if err := store.db.Create(&r1).Error; err != nil {
							t.Fatalf("failed to create record: %v", err)
						}

						recordId := r1.Id
						// For the "delete non-existing" test, use a non-existing ID
						if tc.name == "delete non-existing record does not error" {
							recordId = 999999
						}

						// Delete the record
						err := store.DeleteRecord(recordId)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}

						// Verify record is deleted (only for existing records)
						if tc.name != "delete non-existing record does not error" {
							var deleted dbRecord
							err := store.db.First(&deleted, r1.Id).Error
							if err == nil {
								t.Errorf("expected record to be deleted, but it still exists")
							}
						}
					})
				}
			})
		}
	})

	t.Run("error cases", func(t *testing.T) {
		tcs := []struct {
			name     string
			recordID uint
			wantErr  string
		}{
			{
				name:     "error on zero id",
				recordID: 0,
				wantErr:  "record id cannot be zero",
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				dbCon := db.ConnDbName("TestDeleteRecord_error")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}

				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						err := store.DeleteRecord(tc.recordID)

						if err == nil {
							t.Fatalf("expected error %q, got none", tc.wantErr)
						}
						if diff := cmp.Diff(tc.wantErr, err.Error()); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					})
				}
			})
		}
	})
}

func TestListRecords(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		tcs := []struct {
			name       string
			SeriesName string
			records    map[string][]Record // key is policy name (main only), value is records for that policy
			start      time.Time
			end        time.Time
			want       []Record // expected output from ListRecords (main retention policy only)
		}{
			{
				name:       "list multiple records from main retention",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
						{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
						{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
					},
				},
				start: time.Time{},
				end:   time.Time{},
				want: []Record{
					{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
					{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
					{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
				},
			},
			{
				name:       "list single record",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-05 00:00:00"), Value: 999.99},
					},
				},
				start: time.Time{},
				end:   time.Time{},
				want: []Record{
					{Time: getDateTime("2022-01-05 00:00:00"), Value: 999.99},
				},
			},
			{
				name:       "list empty series",
				SeriesName: "banana",
				records:    map[string][]Record{"main": {}},
				start:      time.Time{},
				end:        time.Time{},
				want:       []Record{},
			},
			{
				name:       "filter with start time only",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
						{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
						{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
						{Time: getDateTime("2022-01-04 00:00:00"), Value: 400.0},
					},
				},
				start: getDateTime("2022-01-02 00:00:00"),
				end:   time.Time{},
				want: []Record{
					{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
					{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
					{Time: getDateTime("2022-01-04 00:00:00"), Value: 400.0},
				},
			},
			{
				name:       "filter with end time only",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
						{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
						{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
						{Time: getDateTime("2022-01-04 00:00:00"), Value: 400.0},
					},
				},
				start: time.Time{},
				end:   getDateTime("2022-01-03 00:00:00"),
				want: []Record{
					{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
					{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
					{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
				},
			},
			{
				name:       "filter with both start and end time",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
						{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
						{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
						{Time: getDateTime("2022-01-04 00:00:00"), Value: 400.0},
					},
				},
				start: getDateTime("2022-01-02 00:00:00"),
				end:   getDateTime("2022-01-03 00:00:00"),
				want: []Record{
					{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
					{Time: getDateTime("2022-01-03 00:00:00"), Value: 300.0},
				},
			},
			{
				name:       "filter returns empty when range has no matches",
				SeriesName: "banana",
				records: map[string][]Record{
					"main": {
						{Time: getDateTime("2022-01-01 00:00:00"), Value: 100.0},
						{Time: getDateTime("2022-01-02 00:00:00"), Value: 200.0},
					},
				},
				start: getDateTime("2022-01-05 00:00:00"),
				end:   getDateTime("2022-01-10 00:00:00"),
				want:  []Record{},
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						dbCon := db.ConnDbName("TestListRecords_" + tc.name)
						store, err := NewRegistry(dbCon)
						if err != nil {
							t.Fatal(err)
						}

						series := dbTimeSeries{Name: tc.SeriesName}
						if err := store.db.Create(&series).Error; err != nil {
							t.Fatalf("failed to create series: %v", err)
						}

						// Create main policy and records
						for policyName, policyRecords := range tc.records {
							policy := dbSamplingPolicy{
								TimeSeriesID:  series.ID,
								Name:          policyName,
								Precision:     time.Minute,
								Retention:     24 * time.Hour,
								AggregationFn: "avg",
							}
							if err := store.db.Create(&policy).Error; err != nil {
								t.Fatalf("failed to create %s policy: %v", policyName, err)
							}

							// Create records for this policy
							for _, rec := range policyRecords {
								dbRec := dbRecord{
									SamplingId: policy.ID,
									Time:       rec.Time,
									Value:      rec.Value,
								}
								if err := store.db.Create(&dbRec).Error; err != nil {
									t.Fatalf("failed to create %s record: %v", policyName, err)
								}
							}
						}

						// Execute ListRecords
						got, err := store.ListRecords(tc.SeriesName, tc.start, tc.end)
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}

						// Add series name to expected records for comparison
						wantWithSeries := make([]Record, len(tc.want))
						for i, rec := range tc.want {
							wantWithSeries[i] = Record{
								Series: tc.SeriesName,
								Time:   rec.Time,
								Value:  rec.Value,
							}
						}

						if diff := cmp.Diff(wantWithSeries, got,
							cmpopts.IgnoreFields(Record{}, "Id"),
							cmpopts.SortSlices(func(a, b Record) bool {
								if !a.Time.Equal(b.Time) {
									return a.Time.Before(b.Time)
								}
								return a.Value < b.Value
							}),
						); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					})
				}
			})
		}
	})

	t.Run("error cases", func(t *testing.T) {
		tcs := []struct {
			name       string
			SeriesName string
			wantErr    string
		}{
			{
				name:       "error on empty name",
				SeriesName: "",
				wantErr:    "series name is required",
			},
			{
				name:       "error on nonexistent series",
				SeriesName: "nonexistent",
				wantErr:    "series not found: record not found",
			},
		}

		for _, db := range testdbs.DBs() {
			t.Run(db.DbType(), func(t *testing.T) {
				for _, tc := range tcs {
					t.Run(tc.name, func(t *testing.T) {
						dbCon := db.ConnDbName("TestListRecords_error_" + tc.name)
						store, err := NewRegistry(dbCon)
						if err != nil {
							t.Fatal(err)
						}

						// Execute ListRecords
						_, err = store.ListRecords(tc.SeriesName, time.Time{}, time.Time{})

						// Verify error
						if err == nil {
							t.Fatalf("expected error %q, got none", tc.wantErr)
						}
						if diff := cmp.Diff(tc.wantErr, err.Error()); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					})
				}
			})
		}
	})
}

func TestRecordAt(t *testing.T) {
	// Main retention has 2025-01-05..2025-01-10. RecordAt returns latest at or before query time.
	tcs := []struct {
		name      string
		series    string
		queryTime time.Time
		wantValue float64
		wantErr   string
	}{
		{name: "before any data", series: "btc_price", queryTime: getDateTime("2024-12-01 00:00:00"), wantErr: "record not found"},
		{name: "exact match main", series: "btc_price", queryTime: getDateTime("2025-01-05 00:00:00"), wantValue: 14000},
		{name: "within main bucket", series: "btc_price", queryTime: getDateTime("2025-01-05 00:00:00").Add(12 * time.Hour), wantValue: 14000},
		{name: "between main records", series: "btc_price", queryTime: getDateTime("2025-01-06 00:00:00").Add(12 * time.Hour), wantValue: 15000},
		{name: "in main range", series: "btc_price", queryTime: getDateTime("2025-01-07 00:00:00"), wantValue: 16000},
		{name: "after last record", series: "btc_price", queryTime: getDateTime("2025-01-15 00:00:00"), wantValue: 19000},
		{name: "series not found", series: "unknown_series", queryTime: getDateTime("2025-01-05 00:00:00"), wantErr: "series not found"},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestRecordAt")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.RegisterSeries(TimeSeries{
				Name: "btc_price",
				Retention: SamplingPolicy{
					Retention: 7 * 24 * time.Hour,
					Precision: time.Hour,
				},
			}); err != nil {
				t.Fatalf("failed to register series: %v", err)
			}

			for _, r := range testRecords {
				if r.Time.Before(getDateTime("2025-01-05 00:00:00")) {
					continue
				}
				if _, err := store.Ingest(r.Series, r.Time, r.Value); err != nil {
					t.Fatalf("failed to ingest record %v: %v", r, err)
				}
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					got, err := store.RecordAt(tc.series, tc.queryTime)
					if tc.wantErr != "" {
						if err == nil {
							t.Fatalf("expected error %q but got none", tc.wantErr)
						}
						if diff := cmp.Diff(tc.wantErr, err.Error()); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if got.Value != tc.wantValue {
						t.Errorf("unexpected value: want %.2f, got %.2f", tc.wantValue, got.Value)
					}
				})
			}
		})
	}
}
