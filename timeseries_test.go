package timeseries

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestMain modifies how test are run,
// it makes sure that the needed DBs are ready and does cleanup in the end.
func TestMain(m *testing.M) {
	testdbs.InitDBS()
	// main block that runs tests
	code := m.Run()
	_ = testdbs.Clean()
	os.Exit(code)
}

func TestRegisterSeries(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			tcs := []struct {
				name     string
				initial  *TimeSeries // optional initial insert
				input    TimeSeries
				expected *TimeSeries // nil for error cases
				wantErr  string
			}{
				{
					name: "create new series",
					input: TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention: 30 * 24 * time.Hour,
							Precision: time.Hour,
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention: 30 * 24 * time.Hour,
							Precision: time.Hour,
						},
					},
				},
				{
					name: "idempotent registration",
					initial: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention: 30 * 24 * time.Hour,
							Precision: time.Hour,
						},
					},
					input: TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention: 30 * 24 * time.Hour,
							Precision: time.Hour,
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention: 30 * 24 * time.Hour,
							Precision: time.Hour,
						},
					},
				},
				{
					name: "register without main policy returns error",
					input: TimeSeries{
						Name: "series_no_main",
					},
					wantErr: "main policy is required",
				},
			}

			dbCon := db.ConnDbName("TestRegisterSeries")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					if tc.initial != nil {
						if err := store.RegisterSeries(*tc.initial); err != nil {
							t.Fatalf("setup initial failed: %v", err)
						}
					}

					err := store.RegisterSeries(tc.input)
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
						t.Fatalf("RegisterSeries failed: %v", err)
					}

					got, err := store.GetSeries(tc.input.Name)
					if err != nil {
						t.Fatalf("GetSeries failed: %v", err)
					}
					if diff := cmp.Diff(*tc.expected, got); diff != "" {
						t.Errorf("unexpected series state (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestListSeries(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestListSeries")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			// Arrange: Create some sample data
			mainPolicy := SamplingPolicy{
				Retention: 30 * 24 * time.Hour,
				Precision: time.Hour,
			}
			s1 := TimeSeries{Name: "btc_price", Retention: mainPolicy}
			s2 := TimeSeries{Name: "eth_price", Retention: mainPolicy}

			if err := store.RegisterSeries(s1); err != nil {
				t.Fatalf("failed to insert s1: %v", err)
			}
			if err := store.RegisterSeries(s2); err != nil {
				t.Fatalf("failed to insert s2: %v", err)
			}

			got, err := store.ListSeries()
			if err != nil {
				t.Fatalf("ListSeries failed: %v", err)
			}

			want := []TimeSeries{s1, s2}

			// Assert
			if diff := cmp.Diff(want, got,
				cmpopts.SortSlices(func(a, b TimeSeries) bool { return a.Name < b.Name }),
			); diff != "" {
				t.Errorf("unexpected ListSeries result (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCleanOneSeries(t *testing.T) {
	base := time.Now().Truncate(time.Hour)
	oldTime := base.Add(-25 * 24 * time.Hour) // 25 days ago
	midTime := base.Add(-5 * 24 * time.Hour)  // 5 days ago
	recentTime := base                        // within retention

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			t.Run("main policy", func(t *testing.T) {
				dbCon := db.ConnDbName("TestCleanOneSeries_main")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}
				seriesName := "btc_price"
				series := TimeSeries{
					Name: seriesName,
					Retention: SamplingPolicy{
						Precision: time.Hour,
						Retention: 7 * 24 * time.Hour,
					},
				}
				if err := store.RegisterSeries(series); err != nil {
					t.Fatalf("RegisterSeries failed: %v", err)
				}
				for _, r := range []struct {
					t time.Time
					v float64
				}{
					{oldTime, 10},
					{midTime, 20},
					{recentTime, 30},
				} {
					if _, err := store.Ingest(seriesName, r.t, r.v); err != nil {
						t.Fatalf("Ingest failed: %v", err)
					}
				}
				var s dbTimeSeries
				if err := store.db.Preload("Policies").Where("name = ?", seriesName).First(&s).Error; err != nil {
					t.Fatalf("load series: %v", err)
				}
				if err := store.cleanOneSeries(t.Context(), s); err != nil {
					t.Fatalf("cleanOneSeries failed: %v", err)
				}
				got, err := store.ListRecords(seriesName, time.Time{}, time.Time{})
				if err != nil {
					t.Fatalf("ListRecords failed: %v", err)
				}
				// Main retention 7 days: 25-day-old removed; mid and recent remain.
				if len(got) != 2 {
					t.Errorf("expected 2 records after cleanup, got %d: %v", len(got), got)
				}
				sort.Slice(got, func(i, j int) bool { return got[i].Time.Before(got[j].Time) })
				want := []Record{
					{Series: seriesName, Time: midTime, Value: 20},
					{Series: seriesName, Time: recentTime, Value: 30},
				}
				if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(Record{}, "Id")); diff != "" {
					t.Errorf("unexpected records (-want +got):\n%s", diff)
				}
			})

		})
	}
}

func TestReduceOneSeries(t *testing.T) {
	tcs := []struct {
		name        string
		precision   time.Duration
		aggregateFn string // policy aggregate name (e.g. AggregateAVG, "sum"); empty = no-op
		records     []struct {
			t time.Time
			v float64
		}
		wantRecords []Record
		wantErr     string
	}{
		{
			name:        "empty aggregate name is no-op",
			precision:   time.Hour,
			aggregateFn: "",
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 10:00:00"), 1},
				{getDateTime("2025-01-01 10:30:00"), 2},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 1},
				{Time: getDateTime("2025-01-01 10:30:00"), Value: 2},
			},
		},
		{
			name:        "empty series no error",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records:     nil,
			wantRecords: []Record{},
		},
		{
			name:        "single record per bucket unchanged",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 09:00:00"), 10},
				{getDateTime("2025-01-01 10:00:00"), 20},
				{getDateTime("2025-01-01 11:00:00"), 30},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 09:00:00"), Value: 10},
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 20},
				{Time: getDateTime("2025-01-01 11:00:00"), Value: 30},
			},
		},
		{
			name:        "single record per unaligned bucket",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 09:02:00"), 10},
				{getDateTime("2025-01-01 09:05:00"), 30},
				{getDateTime("2025-01-01 10:03:00"), 20},
				{getDateTime("2025-01-01 11:04:00"), 30},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 09:00:00"), Value: 20},
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 20},
				{Time: getDateTime("2025-01-01 11:00:00"), Value: 30},
			},
		},
		{
			name:        "two records same bucket reduced to avg",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 12:00:00"), 10},
				{getDateTime("2025-01-01 12:30:00"), 20},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 12:00:00"), Value: 15},
			},
		},
		{
			name:        "multiple buckets mixed single and multiple",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 16:00:00"), 100},
				{getDateTime("2025-01-01 17:00:00"), 10},
				{getDateTime("2025-01-01 17:30:00"), 20},
				{getDateTime("2025-01-01 18:00:00"), 1},
				{getDateTime("2025-01-01 18:15:00"), 2},
				{getDateTime("2025-01-01 18:45:00"), 3},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 16:00:00"), Value: 100},
				{Time: getDateTime("2025-01-01 17:00:00"), Value: 15},
				{Time: getDateTime("2025-01-01 18:00:00"), Value: 2},
			},
		},
		{
			name:        "two records same bucket reduced to max",
			precision:   time.Hour,
			aggregateFn: "max",
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 15:00:00"), 3},
				{getDateTime("2025-01-01 15:30:00"), 8},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 15:00:00"), Value: 8},
			},
		},
		{
			name:        "10 min precision two records in bucket reduced to avg",
			precision:   10 * time.Minute,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 10:02:00"), 5},
				{getDateTime("2025-01-01 10:07:00"), 15},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 10},
			},
		},
		{
			name:        "10 min precision single record per unaligned bucket",
			precision:   10 * time.Minute,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 10:03:00"), 10},
				{getDateTime("2025-01-01 10:15:00"), 20},
				{getDateTime("2025-01-01 10:28:00"), 30},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 10},
				{Time: getDateTime("2025-01-01 10:10:00"), Value: 20},
				{Time: getDateTime("2025-01-01 10:20:00"), Value: 30},
			},
		},
		{
			name:        "10 min precision three in one bucket one in another reduced to avg",
			precision:   10 * time.Minute,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 14:01:00"), 10},
				{getDateTime("2025-01-01 14:04:00"), 20},
				{getDateTime("2025-01-01 14:09:00"), 30},
				{getDateTime("2025-01-01 14:25:00"), 100},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 14:00:00"), Value: 20},
				{Time: getDateTime("2025-01-01 14:20:00"), Value: 100},
			},
		},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestReduceOneSeries")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}
			store.RegisterAggregateFn("max", func(values []float64) float64 {
				m := values[0]
				for _, v := range values[1:] {
					if v > m {
						m = v
					}
				}
				return m
			})

			for i, tc := range tcs {
				seriesName := fmt.Sprintf("s%d", i)
				t.Run(tc.name, func(t *testing.T) {
					series := TimeSeries{
						Name: seriesName,
						Retention: SamplingPolicy{
							Precision:   tc.precision,
							Retention:   24 * time.Hour,
							AggregateFn: tc.aggregateFn,
						},
					}
					if err := store.RegisterSeries(series); err != nil {
						t.Fatalf("RegisterSeries: %v", err)
					}
					for _, r := range tc.records {
						if _, err := store.Ingest(seriesName, r.t, r.v); err != nil {
							t.Fatalf("Ingest: %v", err)
						}
					}

					err := store.reduceOneSeries(t.Context(), seriesName)
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
						t.Fatalf("reduceOneSeries: %v", err)
					}

					got, err := store.ListRecords(seriesName, time.Time{}, time.Time{})
					if err != nil {
						t.Fatalf("ListRecords: %v", err)
					}
					wantRecords := make([]Record, len(tc.wantRecords))
					for j, r := range tc.wantRecords {
						wantRecords[j] = r
						wantRecords[j].Series = seriesName
					}
					opts := cmp.Options{
						cmpopts.SortSlices(func(a, b Record) bool { return a.Time.Before(b.Time) }),
						cmpopts.IgnoreFields(Record{}, "Id"),
					}
					if diff := cmp.Diff(wantRecords, got, opts); diff != "" {
						t.Errorf("records (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

// TestReduceOneSeriesBatched verifies that reduceOneSeries correctly processes data spanning
// multiple chunks (more than reduceBucketBatchSize buckets).
func TestReduceOneSeriesBatched(t *testing.T) {
	// reduceBucketBatchSize is 100 in timeseries.go; use 101 and 201 to span 2 and 3 chunks.
	tcs := []struct {
		name       string
		numBuckets int // number of 1h buckets; >100 triggers multiple chunks
	}{
		{"two chunks", reduceBucketBatchSize + 1},
		{"three chunks", reduceBucketBatchSize*2 + 1},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestReduceOneSeriesBatched")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					seriesName := fmt.Sprintf("batched_%d", i)
					base := getDateTime("2025-01-01 00:00:00")
					records, wantRecords := buildBatchedReduceInput(base, tc.numBuckets)

					series := TimeSeries{
						Name: seriesName,
						Retention: SamplingPolicy{
							Precision:   time.Hour,
							Retention:   24 * time.Hour,
							AggregateFn: AggregateAVG,
						},
					}
					if err := store.RegisterSeries(series); err != nil {
						t.Fatalf("RegisterSeries: %v", err)
					}
					for _, r := range records {
						if _, err := store.Ingest(seriesName, r.t, r.v); err != nil {
							t.Fatalf("Ingest: %v", err)
						}
					}

					if err := store.reduceOneSeries(t.Context(), seriesName); err != nil {
						t.Fatalf("reduceOneSeries: %v", err)
					}

					got, err := store.ListRecords(seriesName, time.Time{}, time.Time{})
					if err != nil {
						t.Fatalf("ListRecords: %v", err)
					}
					wantWithSeries := make([]Record, len(wantRecords))
					for j, r := range wantRecords {
						wantWithSeries[j] = r
						wantWithSeries[j].Series = seriesName
					}
					opts := cmp.Options{
						cmpopts.SortSlices(func(a, b Record) bool { return a.Time.Before(b.Time) }),
						cmpopts.IgnoreFields(Record{}, "Id"),
					}
					if diff := cmp.Diff(wantWithSeries, got, opts); diff != "" {
						t.Errorf("records (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

// buildBatchedReduceInput builds numBuckets hours of data: bucket 0 and last bucket have two
// points (reduced to avg), others have one. Returns records to ingest and expected after reduce.
func buildBatchedReduceInput(base time.Time, numBuckets int) (
	records []struct {
		t time.Time
		v float64
	},
	wantRecords []Record,
) {
	hour := time.Hour
	for i := 0; i <= numBuckets; i++ {
		t := base.Add(time.Duration(i) * hour)
		switch i {
		case 0:
			records = append(records,
				struct {
					t time.Time
					v float64
				}{t, 1},
				struct {
					t time.Time
					v float64
				}{t.Add(30 * time.Minute), 3},
			)
			wantRecords = append(wantRecords, Record{Time: t, Value: 2})
		case numBuckets:
			records = append(records,
				struct {
					t time.Time
					v float64
				}{t, 5},
				struct {
					t time.Time
					v float64
				}{t.Add(45 * time.Minute), 7},
			)
			wantRecords = append(wantRecords, Record{Time: t, Value: 6})
		default:
			records = append(records, struct {
				t time.Time
				v float64
			}{t.Add(15 * time.Minute), float64(10 + i)})
			wantRecords = append(wantRecords, Record{Time: t, Value: float64(10 + i)})
		}
	}
	return records, wantRecords
}

// TestReduceOneSeriesMultipleCalls verifies that calling reduceOneSeries multiple times on the same
// data is idempotent: the first call reduces and aligns; further calls leave the data unchanged.
func TestReduceOneSeriesMultipleCalls(t *testing.T) {
	tcs := []struct {
		name        string
		precision   time.Duration
		aggregateFn string
		records     []struct {
			t time.Time
			v float64
		}
		wantRecords []Record // expected after every reduce call (1st, 2nd, 3rd)
	}{
		{
			name:        "hour precision two in bucket then idempotent",
			precision:   time.Hour,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 12:00:00"), 10},
				{getDateTime("2025-01-01 12:30:00"), 20},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 12:00:00"), Value: 15},
			},
		},
		{
			name:        "odd duration 90 min two in bucket then idempotent",
			precision:   90 * time.Minute,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 01:10:00"), 10},
				{getDateTime("2025-01-01 01:20:00"), 20},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 00:00:00"), Value: 15},
			},
		},
		{
			name:        "10 min precision mixed buckets then idempotent",
			precision:   10 * time.Minute,
			aggregateFn: AggregateAVG,
			records: []struct {
				t time.Time
				v float64
			}{
				{getDateTime("2025-01-01 10:02:00"), 5},
				{getDateTime("2025-01-01 10:07:00"), 15},
				{getDateTime("2025-01-01 10:25:00"), 100},
			},
			wantRecords: []Record{
				{Time: getDateTime("2025-01-01 10:00:00"), Value: 10},
				{Time: getDateTime("2025-01-01 10:20:00"), Value: 100},
			},
		},
	}

	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			dbCon := db.ConnDbName("TestReduceOneSeriesMultipleCalls")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			for i, tc := range tcs {
				seriesName := fmt.Sprintf("s%d", i)
				t.Run(tc.name, func(t *testing.T) {
					series := TimeSeries{
						Name: seriesName,
						Retention: SamplingPolicy{
							Precision:   tc.precision,
							Retention:   24 * time.Hour,
							AggregateFn: tc.aggregateFn,
						},
					}
					if err := store.RegisterSeries(series); err != nil {
						t.Fatalf("RegisterSeries: %v", err)
					}
					for _, r := range tc.records {
						if _, err := store.Ingest(seriesName, r.t, r.v); err != nil {
							t.Fatalf("Ingest: %v", err)
						}
					}

					wantRecords := make([]Record, len(tc.wantRecords))
					for j, r := range tc.wantRecords {
						wantRecords[j] = r
						wantRecords[j].Series = seriesName
					}
					opts := cmp.Options{
						cmpopts.SortSlices(func(a, b Record) bool { return a.Time.Before(b.Time) }),
						cmpopts.IgnoreFields(Record{}, "Id"),
					}

					for call := 0; call < 3; call++ {
						if err := store.reduceOneSeries(t.Context(), seriesName); err != nil {
							t.Fatalf("reduceOneSeries call %d: %v", call+1, err)
						}
						got, err := store.ListRecords(seriesName, time.Time{}, time.Time{})
						if err != nil {
							t.Fatalf("ListRecords after call %d: %v", call+1, err)
						}
						if diff := cmp.Diff(wantRecords, got, opts); diff != "" {
							t.Errorf("after reduce call %d, records (-want +got):\n%s", call+1, diff)
						}
					}
				})
			}
		})
	}
}

// returns a pointer to a specific type
func ptr[T any](v T) *T {
	return &v
}

func getDateTime(datetimeStr string) time.Time {
	parsedTime, err := time.Parse("2006-01-02 15:04:05", datetimeStr)
	if err != nil {
		panic(fmt.Errorf("unable to parse datetime: %v", err))
	}
	return parsedTime.UTC()
}
