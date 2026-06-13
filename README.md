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

ctx := context.Background()
_ = ts.DefineSeries(ctx, timeseries.Series{
	Name:      "AAPL",
	Precision: 24 * time.Hour,
	Retention: 10 * 365 * 24 * time.Hour,
	Fields: []timeseries.Field{
		{Name: "close", Aggregate: timeseries.AggLast},
		{Name: "high", Aggregate: timeseries.AggMax},
	},
})

day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
_ = ts.Write(ctx, "AAPL", timeseries.Point{Time: day, Values: map[string]float64{"close": 102.1, "high": 103.0}})

v, _, _ := ts.FieldAt(ctx, "AAPL", "close", day)
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

A `Series` is a named stream with its own precision, retention, and fields:

```go
type Series struct {
	Name      string
	Precision time.Duration // bucket size, minimum 1 second
	Retention time.Duration // how long data is kept
	Fields    []Field       // the fields this series carries
}

func (s *Store) DefineSeries(ctx context.Context, cfg Series) error   // create or update by name; syncs fields
func (s *Store) GetSeries(ctx context.Context, name string) (Series, error)
func (s *Store) ListSeries(ctx context.Context) ([]Series, error)
func (s *Store) DropSeries(ctx context.Context, name string) error    // removes the series, its fields, and all its records
```

`DefineSeries` syncs the series' fields **declaratively**: `cfg.Fields` is the complete
desired set. Fields present in the store but absent from `cfg.Fields` are removed and
their records deleted; new fields are created; existing fields' aggregates are updated.

### Fields

A `Field` is a series-scoped measurement name plus the aggregate used when reducing a
precision bucket. Each series owns its fields, so the same name (e.g. `close`) in two
series is two independent fields with independent aggregates. Fields are declared inside
`DefineSeries` (above) — there is no standalone field API.

```go
type Field struct {
	Name      string
	Aggregate string // one of the Agg* constants, or "" for no bucket reduction
}
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

`DefineSeries` errors if any field's aggregate name is non-empty and has not been registered.

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
_ = ts.DefineSeries(ctx, timeseries.Series{
	Name:      "AAPL",
	Precision: 24 * time.Hour,
	Retention: 10 * 365 * 24 * time.Hour,
	Fields:    []timeseries.Field{{Name: "spread", Aggregate: "range"}},
})
```

### Writing

```go
type Point struct {
	Time   time.Time
	Values map[string]float64 // field name -> value
}

func (s *Store) Write(ctx context.Context, series string, p Point) error
func (s *Store) WriteMany(ctx context.Context, series string, ps []Point) error
```

`Write`/`WriteMany` upsert on `(series, field, time)`: writing the same field at the same
timestamp overwrites the existing value. `WriteMany` resolves and validates every point's
time and field before any row is written, and applies the whole batch in one transaction —
so a write lands in full or not at all, which makes it suitable for bulk backfilling.

### Reading

```go
// Multi-field points in [start, end], time-ascending. Records sharing a timestamp
// are pivoted into one Point.
func (s *Store) Range(ctx context.Context, series string, start, end time.Time) ([]Point, error)

// As-of snapshot: each field's latest value at or before t. Point.Time is t.
func (s *Store) At(ctx context.Context, series string, t time.Time) (Point, error)

// One field's scalar samples in [start, end], time-ascending.
func (s *Store) FieldRange(ctx context.Context, series, field string, start, end time.Time) ([]Sample, error)

// One field's latest value at or before t; the bool reports whether a value was found.
func (s *Store) FieldAt(ctx context.Context, series, field string, t time.Time) (float64, bool, error)

type Sample struct {
	Time  time.Time
	Value float64
}
```

Pass a zero `time.Time` for an unbounded start or end.

### Deleting

```go
func (s *Store) Delete(ctx context.Context, series string, t time.Time) error                // all fields at exactly t
func (s *Store) DeleteRange(ctx context.Context, series string, start, end time.Time) error   // all records in [start, end]
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

### Concurrency

A single `*Store` is safe for concurrent use. Reads and point writes run concurrently;
the structural operations — `DefineSeries`, `DropSeries`, `Maintain`, and
`RegisterAggregate` — take an exclusive lock and run one at a time, blocking reads and
writes for their duration. This is what prevents a field deletion or a maintenance pass
from racing a concurrent write into orphaned or lost records.

The lock is per-`Store`. It does **not** coordinate across multiple `Store` instances or
other processes pointed at the same database; application-level integrity assumes all
writes go through one `Store`. Because `Maintain` holds the lock for the whole sweep, run
it from a dedicated maintenance goroutine, not on a hot read/write path.

### Errors

Missing series and undefined fields are reported through sentinel errors you can match
with `errors.Is`:

```go
var ErrSeriesNotFound = errors.New("series not found")
var ErrFieldNotFound  = errors.New("field not found")
```

```go
if _, err := ts.GetSeries(ctx, "UNKNOWN"); errors.Is(err, timeseries.ErrSeriesNotFound) {
	// define it
}
```

## Migrating from v0.1

`v0.2.0` is a breaking redesign. The scalar `(series, time, value)` model and the
`Registry` type are gone, replaced by a field-dimensioned `(series, field, time) → value`
model behind a `Store`. There is **no in-place migration path** — the storage schema is
incompatible and existing data must be re-ingested through the new API.

Key changes:

- **`Registry` → `Store`** (`timeseries.New` now returns `*Store`).
- **Points carry named fields.** A point is now `Point{Time, Values map[string]float64}`
  instead of a single scalar value. A single-value series is just a series with one field.
- **Fields are declared in `DefineSeries`** via `Series.Fields` and synced declaratively;
  there is no standalone field API.
- **Aggregation is per-field** (`Field.Aggregate`); the multi-policy `SamplingPolicy`
  concept has been removed. Precision and retention live on the series.
- **Every method takes a `context.Context`** as its first argument.
