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
