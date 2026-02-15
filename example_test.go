package timeseries_test

import (
	"fmt"
	"time"

	"github.com/go-bumbu/timeseries"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func ExampleRegistry_e2e() {

	// Create the registry (this also runs migrations)
	registry, err := timeseries.NewRegistry(getDb())
	if err != nil {
		fmt.Printf("failed to create registry: %v\n", err)
		return
	}

	seriesName := "btc_price"

	// Register a time series with retention policy
	series := timeseries.TimeSeries{
		Name: seriesName,
		Retention: timeseries.SamplingPolicy{
			Precision: time.Minute,
			Retention: 24 * time.Hour, // 1 day for raw data
		},
	}

	if err := registry.RegisterSeries(series); err != nil {
		fmt.Printf("failed to register series: %v\n", err)
		return
	}
	fmt.Println("Series registered successfully")

	// Ingest some data points
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []timeseries.Record{
		{Series: seriesName, Time: baseTime, Value: 42000.0},
		{Series: seriesName, Time: baseTime.Add(24 * time.Hour), Value: 43500.0},
		{Series: seriesName, Time: baseTime.Add(48 * time.Hour), Value: 41000.0},
		{Series: seriesName, Time: baseTime.Add(72 * time.Hour), Value: 44000.0},
	}

	for _, r := range records {
		if _, err := registry.Ingest(r.Series, r.Time, r.Value); err != nil {
			fmt.Printf("failed to ingest record: %v\n", err)
			return
		}
	}
	fmt.Printf("Ingested %d records\n", len(records))

	// List all records for the series
	allRecords, err := registry.ListRecords(seriesName, time.Time{}, time.Time{})
	if err != nil {
		fmt.Printf("failed to list records: %v\n", err)
		return
	}
	fmt.Printf("Total records in series: %d\n", len(allRecords))

	// Query value at a specific point in time
	queryTime := baseTime.Add(36 * time.Hour) // Between 2nd and 3rd record
	value, err := registry.ValueAt(seriesName, queryTime)
	if err != nil {
		fmt.Printf("failed to get value: %v\n", err)
		return
	}
	fmt.Printf("Value at query time: %.2f\n", value)

	// Output:
	// Series registered successfully
	// Ingested 4 records
	// Total records in series: 4
	// Value at query time: 43500.00
}

func getDb() *gorm.DB {
	// Create an in-memory SQLite database
	sqliteDb, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic("failed to connect database")
	}
	return sqliteDb
}
