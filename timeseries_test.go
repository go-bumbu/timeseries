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
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				{
					name: "idempotent registration",
					initial: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					input: TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				{
					name: "update retention and policy",
					initial: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					input: TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 10 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 10 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				{
					name: "delete policy",
					initial: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					input: TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "btc_price",
						Retention: SamplingPolicy{
							Retention:     30 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				// Downsampling precision validation: valid and error cases
				{
					name: "downsampling precision within main retention",
					input: TimeSeries{
						Name: "series_ok",
						Retention: SamplingPolicy{
							Retention:     7 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "series_ok",
						Retention: SamplingPolicy{
							Retention:     7 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: time.Hour, Retention: 24 * time.Hour, AggregationFn: nil},
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				{
					name: "downsampling precision equals main retention",
					input: TimeSeries{
						Name: "series_edge",
						Retention: SamplingPolicy{
							Retention:     7 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: 7 * 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					expected: &TimeSeries{
						Name: "series_edge",
						Retention: SamplingPolicy{
							Retention:     7 * 24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: 7 * 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
				},
				{
					name: "downsampling precision exceeds main retention returns error",
					input: TimeSeries{
						Name: "series_bad",
						Retention: SamplingPolicy{
							Retention:     24 * time.Hour,
							Precision:     time.Hour,
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{Precision: 7 * 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
					},
					wantErr: "downsampling policy precision",
				},
				{
					name: "register without main policy returns error",
					input: TimeSeries{
						Name: "series_no_main",
						DownSampling: []SamplingPolicy{
							{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
						},
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
					sort.Slice(got.DownSampling, func(i, j int) bool {
						return got.DownSampling[i].Precision < got.DownSampling[j].Precision
					})
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
				Retention:     30 * 24 * time.Hour,
				Precision:     time.Hour,
				AggregationFn: nil,
			}
			s1 := TimeSeries{
				Name:      "btc_price",
				Retention: mainPolicy,
				DownSampling: []SamplingPolicy{
					{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
					{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
				},
			}
			s2 := TimeSeries{
				Name:      "eth_price",
				Retention: mainPolicy,
				DownSampling: []SamplingPolicy{
					{Precision: time.Hour, Retention: 14 * 24 * time.Hour, AggregationFn: nil},
				},
			}

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

			// Sort the policies for comparison (in case DB doesn't guarantee order)
			//for i := range got {
			//	sort.Slice(got[i].DownSampling, func(a, b int) bool {
			//		return got[i].DownSampling[a].Name > got[i].DownSampling[b].Name
			//	})
			//}

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
						Precision:     time.Hour,
						Retention:     7 * 24 * time.Hour,
						AggregationFn: nil,
					},
				}
				if err := store.RegisterSeries(series); err != nil {
					t.Fatalf("RegisterSeries failed: %v", err)
				}
				for _, r := range []Record{
					{Series: seriesName, Time: oldTime, Value: 10},
					{Series: seriesName, Time: midTime, Value: 20},
					{Series: seriesName, Time: recentTime, Value: 30},
				} {
					if err := store.Ingest(r); err != nil {
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

			t.Run("downsampling policy", func(t *testing.T) {
				dbCon := db.ConnDbName("TestCleanOneSeries_downsampling")
				store, err := NewRegistry(dbCon)
				if err != nil {
					t.Fatal(err)
				}
				seriesName := "eth_price"
				series := TimeSeries{
					Name: seriesName,
					Retention: SamplingPolicy{
						Precision:     time.Hour,
						Retention:     7 * 24 * time.Hour,
						AggregationFn: nil,
					},
					DownSampling: []SamplingPolicy{
						{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
					},
				}
				if err := store.RegisterSeries(series); err != nil {
					t.Fatalf("RegisterSeries failed: %v", err)
				}
				var s dbTimeSeries
				if err := store.db.Preload("Policies").Where("name = ?", seriesName).First(&s).Error; err != nil {
					t.Fatalf("load series: %v", err)
				}
				var downID uint
				for _, p := range s.Policies {
					if p.Name != mainPolicyName {
						downID = p.ID
						break
					}
				}
				// Insert directly into downsampling: one older than 30d, one within 30d.
				for _, rec := range []dbRecord{
					{SamplingId: downID, Time: base.Add(-35 * 24 * time.Hour), Value: 100},
					{SamplingId: downID, Time: midTime, Value: 200},
				} {
					if err := store.db.Create(&rec).Error; err != nil {
						t.Fatalf("create downsampling record: %v", err)
					}
				}
				if err := store.cleanOneSeries(t.Context(), s); err != nil {
					t.Fatalf("cleanOneSeries failed: %v", err)
				}
				got, err := store.ListRecords(seriesName, time.Time{}, time.Time{})
				if err != nil {
					t.Fatalf("ListRecords failed: %v", err)
				}
				// Downsampling retention 30 days: 35-day-old removed; mid remains.
				if len(got) != 1 {
					t.Errorf("expected 1 record after cleanup, got %d: %v", len(got), got)
				}
				if len(got) == 1 {
					want := Record{Series: seriesName, Time: midTime, Value: 200}
					if diff := cmp.Diff(want, got[0], cmpopts.IgnoreFields(Record{}, "Id")); diff != "" {
						t.Errorf("unexpected record (-want +got):\n%s", diff)
					}
				}
			})
		})
	}
}

func TestDownsampleOne(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {

			tcs := []struct {
				name     string
				series  TimeSeries // optional initial insert
				wantErr  string
			}{
				{
					name:     "simple downsample",
					series:  TimeSeries{
						Name:         "tc1",
						Retention:    SamplingPolicy{
							Retention:     24* time.Hour, // 1 day
							Precision:      10 * time.Minute, // every 10 minutes
							AggregationFn: nil,
						},
						DownSampling: []SamplingPolicy{
							{
								Retention:     30*24* time.Hour, // 30 day
								Precision:     time.Hour, // hour value
								AggregationFn: nil,
							},
							{
								Retention:     30*24* time.Hour, // 60 day
								Precision:      24* time.Hour, // daily value
								AggregationFn: nil,
							},
						},
					},
					wantErr:  "",
				},
			}

			dbCon := db.ConnDbName("TestDownsampleOne")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					series :=

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
					sort.Slice(got.DownSampling, func(i, j int) bool {
						return got.DownSampling[i].Precision < got.DownSampling[j].Precision
					})
					if diff := cmp.Diff(*tc.expected, got); diff != "" {
						t.Errorf("unexpected series state (-want +got):\n%s", diff)
					}
				})
			}

			dbCon := db.ConnDbName("TestListSeries")
			store, err := NewRegistry(dbCon)
			if err != nil {
				t.Fatal(err)
			}

			// Arrange: Create some sample data
			mainPolicy := SamplingPolicy{
				Retention:     30 * 24 * time.Hour,
				Precision:     time.Hour,
				AggregationFn: nil,
			}
			s1 := TimeSeries{
				Name:      "btc_price",
				Retention: mainPolicy,
				DownSampling: []SamplingPolicy{
					{Precision: time.Hour, Retention: 7 * 24 * time.Hour, AggregationFn: nil},
					{Precision: 24 * time.Hour, Retention: 30 * 24 * time.Hour, AggregationFn: nil},
				},
			}
			s2 := TimeSeries{
				Name:      "eth_price",
				Retention: mainPolicy,
				DownSampling: []SamplingPolicy{
					{Precision: time.Hour, Retention: 14 * 24 * time.Hour, AggregationFn: nil},
				},
			}

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

			// Sort the policies for comparison (in case DB doesn't guarantee order)
			//for i := range got {
			//	sort.Slice(got[i].DownSampling, func(a, b int) bool {
			//		return got[i].DownSampling[a].Name > got[i].DownSampling[b].Name
			//	})
			//}

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

// returns a pointer to a specific type
func ptr[T any](v T) *T {
	return &v
}
func getDate(timeStr string) time.Time {
	// Parse the string based on the provided layout
	parsedTime, err := time.Parse("2006-01-02", timeStr)
	if err != nil {
		panic(fmt.Errorf("unable to parse time: %v", err))
	}
	return parsedTime
}

func getdatetime(datetimeStr string) time.Time {
	parsedTime, err := time.Parse("2006-01-02 15:04:05", datetimeStr)
	if err != nil {
		panic(fmt.Errorf("unable to parse datetime: %v", err))
	}
	return parsedTime.UTC()
}
