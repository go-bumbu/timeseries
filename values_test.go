package timeseries

import (
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
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
				if err := store.Ingest(r); err != nil {
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
