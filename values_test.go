package timeseries

import (
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

var testRecords = []Record{
	{Series: "btc_price", Time: getDate("2025-01-01"), Value: 10000},
	{Series: "btc_price", Time: getDate("2025-01-02"), Value: 11000},
	{Series: "btc_price", Time: getDate("2025-01-03"), Value: 12000},
	{Series: "btc_price", Time: getDate("2025-01-04"), Value: 13000},
	{Series: "btc_price", Time: getDate("2025-01-05"), Value: 14000},
	{Series: "btc_price", Time: getDate("2025-01-06"), Value: 15000},
	{Series: "btc_price", Time: getDate("2025-01-07"), Value: 16000},
	{Series: "btc_price", Time: getDate("2025-01-08"), Value: 17000},
	{Series: "btc_price", Time: getDate("2025-01-09"), Value: 18000},
	{Series: "btc_price", Time: getDate("2025-01-10"), Value: 19000},
}

func TestRecordAt(t *testing.T) {
	// Single setup: main retention has 2025-01-05..2025-01-10 (short retention, rest "cleaned");
	// downsampling has 2024-12-15 and 2024-12-20 (longer retention). RecordAt returns latest at or before query time across all policies.
	tcs := []struct {
		name      string
		series    string
		queryTime time.Time
		wantValue float64
		wantErr   string
	}{
		{name: "before any data", series: "btc_price", queryTime: getDate("2024-12-01"), wantErr: "record not found"},
		{name: "downsampling only (older bucket)", series: "btc_price", queryTime: getDate("2024-12-18"), wantValue: 8500},
		{name: "downsampling only (latest bucket)", series: "btc_price", queryTime: getDate("2024-12-31"), wantValue: 9000},
		{name: "exact match main", series: "btc_price", queryTime: getDate("2025-01-05"), wantValue: 14000},
		{name: "prioritize main policy", series: "btc_price", queryTime: getDate("2025-01-05").Add(12 * time.Hour), wantValue: 14000},
		{name: "between main records", series: "btc_price", queryTime: getDate("2025-01-06").Add(12 * time.Hour), wantValue: 15000},
		{name: "in main range", series: "btc_price", queryTime: getDate("2025-01-07"), wantValue: 16000},
		{name: "after last record", series: "btc_price", queryTime: getDate("2025-01-15"), wantValue: 19000},
		{name: "series not found", series: "unknown_series", queryTime: getDate("2025-01-05"), wantErr: "series not found"},
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
					Retention:     7 * 24 * time.Hour,
					Precision:     time.Hour,
					AggregationFn: nil,
				},
				DownSampling: []SamplingPolicy{
					{Retention: 30 * 24 * time.Hour, Precision: 24 * time.Hour, AggregationFn: nil},
				},
			}); err != nil {
				t.Fatalf("failed to register series: %v", err)
			}

			var s dbTimeSeries
			if err := store.db.Preload("Policies").Where("name = ?", "btc_price").First(&s).Error; err != nil {
				t.Fatalf("failed to load series: %v", err)
			}

			for _, r := range testRecords {
				if r.Time.Before(getDate("2025-01-05")) {
					continue
				}
				if err := store.Ingest(r); err != nil {
					t.Fatalf("failed to ingest record %v: %v", r, err)
				}
			}

			var downsamplingID uint
			for _, p := range s.Policies {
				if p.Name != mainPolicyName {
					downsamplingID = p.ID
					break
				}
			}
			for _, rec := range []dbRecord{
				{SamplingId: downsamplingID, Time: getDate("2024-12-15"), Value: 8500},
				{SamplingId: downsamplingID, Time: getDate("2024-12-20"), Value: 9000},
				{SamplingId: downsamplingID, Time: getDate("2025-01-05"), Value: 14500},
			} {
				if err := store.db.Create(&rec).Error; err != nil {
					t.Fatalf("failed to create downsampling record: %v", err)
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
