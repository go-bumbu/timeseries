# Timeseries

Timeseries is a lightweight time series storage library for Go. It provides a simple API to store and query 
(time, value) data with configurable retention, precision, and optional bucket aggregation. 
It uses [GORM](https://gorm.io) and works with any supported database (SQLite, PostgreSQL, MySQL).

## Features

- **Multiple time series** – Register and manage many independent series in one registry
- **Per-series precision and retention** – Each series has its own precision (time bucket) and retention period
- **Bulk ingestion for backfilling** – Ingest many points at once with `IngestBulk` for historical data
- **Custom aggregation** – Use a custom function to aggregate values within each precision bucket (e.g. avg, sum)

## Installation

```bash
go get github.com/go-bumbu/timeseries
```

## Quick Start

```go

	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	registry, err := timeseries.NewRegistry(db)
	if err != nil {
		panic(err)
	}

	// Register a series: 1-hour precision, 30-day retention, avg for bucket reduction
	series := timeseries.TimeSeries{
		Name: "btc_price",
		Retention: timeseries.SamplingPolicy{
			Precision:   time.Hour,
			Retention:   30 * 24 * time.Hour,
			AggregateFn: timeseries.AggregateAVG,
		},
	}
	if err := registry.RegisterSeries(series); err != nil {
		panic(err)
	}

	// Ingest data
	if _, err := registry.Ingest("btc_price", time.Now(), 42000.0); err != nil {
		panic(err)
	}

	// Value at a specific time (latest at or before that time)
	value, err := registry.ValueAt("btc_price", t.Add(30*time.Minute))
	if err != nil {
		panic(err)
	}
	_ = value

	// Run maintenance (retention cleanup + bucket reduction)
	if err := registry.Maintenance(context.Background()); err != nil {
		panic(err)
	}

```

## Configuration

Each series has a single policy that defines how data is stored and reduced:

```go
timeseries.SamplingPolicy{
	Precision:   time.Hour,           // Time bucket size (min 1 second)
	Retention:   30 * 24 * time.Hour, // How long to keep data
	AggregateFn: timeseries.AggregateAVG, // "avg", or "" for no reduction; register custom with RegisterAggregateFn
}
```

## How To

### Register a custom aggregate

Use a name (e.g. `"sum"`, `"max"`) in `SamplingPolicy.AggregateFn` and register the function with the registry:

```go
registry.RegisterAggregateFn("sum", func(values []float64) float64 {
	var s float64
	for _, v := range values {
		s += v
	}
	return s
})

// Then use in a series
series := timeseries.TimeSeries{
	Name: "events",
	Retention: timeseries.SamplingPolicy{
		Precision:   time.Hour,
		Retention:   7 * 24 * time.Hour,
		AggregateFn: "sum",
	},
}
```

### Run maintenance periodically

Maintenance deletes data older than retention and reduces buckets (when `AggregateFn` is set). Run it on a schedule (e.g. cron or a ticker):

```go
ctx := context.Background()
if err := registry.Maintenance(ctx); err != nil {
	log.Printf("Maintenance failed: %v", err)
}
```

Errors from each series are combined and returned; maintenance does not stop on the first failure.

### Query by time range

Use zero times for no bound:

```go
// All records for the series
all, err := registry.ListRecords("btc_price", time.Time{}, time.Time{})
```

### Get value or record at a specific time

`ValueAt` returns the latest value at or before the given time; `RecordAt` returns the full record:

```go
queryTime := time.Date(2025, 1, 1, 14, 30, 0, 0, time.UTC)
value, err := registry.ValueAt("btc_price", queryTime)

record, err := registry.RecordAt("btc_price", queryTime)
if record != nil {
	// record.Time, record.Value, record.Id
}
```
