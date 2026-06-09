# Timeseries

Timeseries is a lightweight time series storage library for Go. It stores multi-field
points (one timestamp with several named values) under independent named series, each
with its own precision and retention, and optional per-field bucket aggregation.
It uses [GORM](https://gorm.io) and works with any supported database (SQLite, PostgreSQL, MySQL).

The library is oriented towards daily end-of-day (EOD) style data — financial instruments,
metrics, and other regularly-sampled values — but works for any precision down to one second.

## Features

- **Multiple named series** – Define and manage many independent series in one store
- **Per-series precision and retention** – Each series has its own precision (time bucket) and retention period
- **Multi-field points** – A single point carries several named fields (e.g. `close`, `high`, `low`), each with its own per-field aggregation
- **Bulk ingest / backfilling** – Write many points at once; writes upsert on `(series, field, time)`, so re-ingesting or backfilling is safe and idempotent
- **Integer-epoch storage** – Timestamps are stored as integer epoch milliseconds in a compact clustered table (SQLite uses `WITHOUT ROWID`)

## Installation

```bash
go get github.com/go-bumbu/timeseries
```

## Quick Start

```go
db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
ts, err := timeseries.New(db)
if err != nil {
	fmt.Println(err)
	return
}

_ = ts.DefineSeries(timeseries.Series{Name: "AAPL", Precision: 24 * time.Hour, Retention: 10 * 365 * 24 * time.Hour})
_ = ts.DefineField(timeseries.Field{Name: "close", Aggregate: timeseries.AggLast})
_ = ts.DefineField(timeseries.Field{Name: "high", Aggregate: timeseries.AggMax})

day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
_ = ts.Write("AAPL", timeseries.Point{Time: day, Values: map[string]float64{"close": 102.1, "high": 103.0}})

v, _, _ := ts.FieldAt("AAPL", "close", day)
fmt.Printf("close=%.1f\n", v)

// Output:
// close=102.1
```

## API

### Setup

```go
func New(db *gorm.DB) (*Store, error)
```

`New` migrates the schema (series, fields, and the clustered records table) and returns a
`*Store` with the built-in aggregates already registered.

### Series

A `Series` is a named stream with its own precision and retention:

```go
type Series struct {
	Name      string
	Precision time.Duration // bucket size, minimum 1 second
	Retention time.Duration // how long data is kept
}

func (s *Store) DefineSeries(cfg Series) error   // create or update by name
func (s *Store) GetSeries(name string) (Series, error)
func (s *Store) ListSeries() ([]Series, error)
func (s *Store) DropSeries(name string) error    // removes the series and all its records
```

### Fields

A `Field` is a globally-defined measurement name plus the aggregate used when reducing a
precision bucket. Fields are shared across all series.

```go
type Field struct {
	Name      string
	Aggregate string // one of the Agg* constants, or "" for no bucket reduction
}

func (s *Store) DefineField(f Field) error // create or update by name
func (s *Store) ListFields() ([]Field, error)
```

The aggregate names are constants:

| Constant    | Behaviour                          |
|-------------|------------------------------------|
| `AggAvg`    | mean of the bucket values          |
| `AggSum`    | sum of the bucket values           |
| `AggMin`    | smallest value in the bucket       |
| `AggMax`    | largest value in the bucket        |
| `AggFirst`  | earliest value in the bucket       |
| `AggLast`   | latest value in the bucket         |
| `""`        | no reduction (raw rows are kept)   |

`DefineField` errors if the aggregate name is non-empty and has not been registered.

### Custom aggregates

```go
func (s *Store) RegisterAggregate(name string, fn AggregateFn)

type AggregateFn func(values []float64) float64
```

Register a function under a name, then reference that name in a field's `Aggregate`.
Values are passed in **ascending time order**, so `first`/`last`-style reductions are
meaningful:

```go
ts.RegisterAggregate("range", func(v []float64) float64 { return v[len(v)-1] - v[0] })
_ = ts.DefineField(timeseries.Field{Name: "spread", Aggregate: "range"})
```

### Writing

```go
type Point struct {
	Time   time.Time
	Values map[string]float64 // field name -> value
}

func (s *Store) Write(series string, p Point) error
func (s *Store) WriteMany(series string, ps []Point) error
```

`Write`/`WriteMany` upsert on `(series, field, time)`: writing the same field at the same
timestamp overwrites the existing value. `WriteMany` validates all points first and writes
them in one transaction, which makes it suitable for bulk backfilling.

### Reading

```go
// Multi-field points in [start, end], time-ascending. Records sharing a timestamp
// are pivoted into one Point.
func (s *Store) Range(series string, start, end time.Time) ([]Point, error)

// As-of snapshot: each field's latest value at or before t. Point.Time is t.
func (s *Store) At(series string, t time.Time) (Point, error)

// One field's scalar samples in [start, end], time-ascending.
func (s *Store) FieldRange(series, field string, start, end time.Time) ([]Sample, error)

// One field's latest value at or before t; the bool reports whether a value was found.
func (s *Store) FieldAt(series, field string, t time.Time) (float64, bool, error)

type Sample struct {
	Time  time.Time
	Value float64
}
```

Pass a zero `time.Time` for an unbounded start or end.

### Deleting

```go
func (s *Store) Delete(series string, t time.Time) error                // all fields at exactly t
func (s *Store) DeleteRange(series string, start, end time.Time) error   // all records in [start, end]
```

### Maintenance

```go
func (s *Store) Maintain(ctx context.Context) error
```

`Maintain` does two things for every series:

1. **Retention cleanup** – deletes records older than the series' retention.
2. **Per-field bucket reduction** – for each field that has an aggregate, collapses all
   records within a precision bucket into a single value using that field's `AggregateFn`.
   Fields with an empty aggregate are left untouched.

Run it on a schedule (a cron job or a ticker). Errors from each series are collected and
joined; maintenance does not stop on the first failure.

```go
ctx := context.Background()
if err := ts.Maintain(ctx); err != nil {
	log.Printf("maintenance failed: %v", err)
}
```
