# go-bumbu/timeseries — project guidance

Lightweight, multi-database (SQLite / PostgreSQL / MySQL) time series library on GORM.

## Current state

Undergoing a breaking redesign (`v0.2.0`) from a scalar `(series, time, value)` model to a
**field-dimensioned** `(series, field, time) → value` model. Full design:
`docs/superpowers/specs/2026-06-09-timeseries-field-dimension-design.md`. Read it before
changing storage, the public API, or aggregation.

## High-level architecture decisions

- **Field-dimension model (external key).** A point has multiple named **fields**
  (e.g. open/high/low/close/volume). Fields are **owned by a series**: `dbField` rows are
  keyed by `(series_id, name)` (unique within a series) and referenced by integer id from
  `dbRecord`. The same field name in two series is two independent fields with independent
  aggregates. A single-value series is just one field.
- **Storage = one row per `(series, field, time)`.** Composite primary key
  `(series_id, time, field_id)`; no synthetic ID, no secondary indexes. Ingestion is an
  idempotent **upsert** on that key.
- **PK ordering is `(series_id, time, field_id)`** — keeps each series contiguous in time
  order (optimal for whole-series, multi-field pivot reads, the dominant access pattern).
- **Time is stored as integer epoch milliseconds** (`INTEGER` on SQLite, `BIGINT`
  elsewhere) via the `unixMilli` custom column type. The public API stays `time.Time`;
  conversion happens only at the storage boundary; values are normalized to UTC.
- **`WITHOUT ROWID` on SQLite only** to cluster rows on the PK. MySQL/InnoDB clusters on
  the PK automatically; PostgreSQL relies on the PK index. Always gate SQLite-specific
  tuning behind `db.Dialector.Name() == "sqlite"`.
- **Aggregation is per-field, and fields are per-series** (`Field.Aggregate`:
  avg/sum/min/max/first/last/custom). The `dbSamplingPolicy` table and the multi-policy
  concept are **removed**; precision + retention live on `Series`, aggregation on `Field`.
- **Fields are declared inside `DefineSeries`** via `Series.Fields` and synced
  **declaratively**: a field absent from `Series.Fields` on a re-`DefineSeries` is deleted
  (cascading its records); new fields are created; aggregates are updated. There is **no**
  standalone `DefineField`/`ListFields`.
- **Public API:** `Store` (was `Registry`) with `Series{...,Fields []Field}`, `Field`,
  `Point{Time,Values}`, `Sample{Time,Value}`. Identity is `(series, field, time)` — no
  record IDs.

## Caveats / gotchas

- **`WITHOUT ROWID` forbids autoincrement.** Migrate `dbRecord` in its **own**
  `AutoMigrate` call with `db.Set("gorm:table_options", "WITHOUT ROWID")`; migrate the
  dimension tables (`dbSeries`, `dbField`, which use autoincrement IDs) separately. You
  cannot `ALTER` a table into `WITHOUT ROWID` — it only works at create time.
- **Aggregate functions receive values in ascending time order.** This is a contract:
  `first` = `values[0]`, `last` = `values[len-1]`. Preserve time ordering in the reducer.
- **No cross-field aggregates.** `AggregateFn` is `func([]float64) float64` — it cannot
  see other fields. Volume-weighted VWAP and similar are out of scope.
- **One precision + retention per series.** Different-cadence fields (daily vs quarterly)
  must be separate series.
- **Bucket reduction only runs in `Maintain`, only when >1 raw point shares a precision
  bucket.** For daily EOD with daily precision it never fires; reads may see sub-bucket
  points between maintenance runs.
- **Integrity is application-level, not DB foreign keys.** There are no FK constraints on
  `records` (required by the multi-DB constraint). `Write`/`WriteMany` resolve (and
  validate) series/field names before insert. `DropSeries` deletes a series' records, then
  its fields, then the series row in one transaction. `DefineSeries`'s declarative sync
  removes absent fields and cascades their records in the same transaction — i.e.
  application-level field deletion exists (there is no public `DropField`). This holds as
  long as all writes go through the `Store` API; it does not protect against out-of-band
  writers.
- **Multi-DB is a hard requirement.** All behavioral tests must run across the
  `testdbs.DBs()` matrix; never assume SQLite-only behavior outside gated tuning.

## Conventions

- Test-first (TDD). Run behavioral tests against all DBs in `testdbs.DBs()`.
- Keep the p90 timing harness in `timeseries_bench_test.go`; guard storage footprint
  (~20 B/row on SQLite) against rowid-table / text-time regressions.
