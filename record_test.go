package timeseries

import (
	"sort"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

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
			wantErr: "timeseries time value cannot be zero",
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

					err := store.Ingest(tc.input)

					if tc.wantErr != "" {
						// Expecting an error
						if err == nil {
							t.Fatalf("expected error: %s, but got none", tc.wantErr)
						}

						if err.Error() != tc.wantErr {
							t.Errorf("expected error: %s, but got %v", tc.wantErr, err.Error())
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
