package timeseries

import (
	"context"
	"fmt"
	"math"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbRecord is the clustered fact table: one row per (series, time, field).
// PK order (series_id, time, field_id) keeps each series contiguous in time,
// optimal for whole-series pivot reads.
type dbRecord struct {
	SeriesID uint      `gorm:"column:series_id;primaryKey;autoIncrement:false"`
	Time     unixMilli `gorm:"primaryKey;autoIncrement:false"`
	FieldID  uint      `gorm:"column:field_id;primaryKey;autoIncrement:false"`
	Value    float64
}

func (dbRecord) TableName() string { return "records" }

// Point is one timestamp with a set of named field values.
//
// Time must be in time.UTC: the library stores and returns UTC and performs no
// zone conversion, so a non-UTC Time is rejected rather than silently converted.
// Time is stored at millisecond resolution — sub-millisecond components are
// truncated, so two instants within the same millisecond resolve to the same
// stored key (a later write overwrites the earlier). Values must be non-empty
// and every value finite (NaN and ±Inf are rejected).
type Point struct {
	Time   time.Time
	Values map[string]float64
}

// recordKey identifies a stored row within a write batch by its millisecond
// timestamp and field id — used to reject duplicate (time, field) keys before
// insert, since the upsert target is that composite key.
type recordKey struct {
	ms  int64
	fid uint
}

// ensureUTC rejects a non-UTC time. The zero time is exempt: it is the unbounded
// sentinel for range bounds (and the write path rejects zero separately).
func ensureUTC(t time.Time) error {
	if t.IsZero() {
		return nil
	}
	if t.Location() != time.UTC {
		return fmt.Errorf("time must be UTC, got location %q", t.Location())
	}
	return nil
}

// Sample is a single (time, value) pair for one field.
type Sample struct {
	Time  time.Time
	Value float64
}

// Coverage reports how much of a series' defined field set a multi-field snapshot
// resolved. It is returned by At and Latest. Single-field reads (FieldAt,
// LatestField) report presence with a bool instead, since coverage there is
// all-or-nothing.
type Coverage int

const (
	// CoverageNone means no defined field resolved: the result Point is empty
	// (the series has no records at or before the query time).
	CoverageNone Coverage = iota
	// CoveragePartial means some, but not all, defined fields resolved — so the
	// Point is missing one or more fields (e.g. a field added after the queried
	// time, or fields with divergent histories).
	CoveragePartial
	// CoverageFull means every defined field resolved.
	CoverageFull
)

func (c Coverage) String() string {
	switch c {
	case CoverageNone:
		return "none"
	case CoveragePartial:
		return "partial"
	case CoverageFull:
		return "full"
	default:
		return fmt.Sprintf("Coverage(%d)", int(c))
	}
}

// coverage classifies a snapshot by how many of the series' defined fields (total)
// resolved (got): 0 is None, all is Full, anything between is Partial.
func coverage(got, total int) Coverage {
	switch {
	case got == 0:
		return CoverageNone
	case got >= total:
		return CoverageFull
	default:
		return CoveragePartial
	}
}

// Write upserts one multi-field point.
func (s *Store) Write(ctx context.Context, series string, p Point) error {
	return s.WriteMany(ctx, series, []Point{p})
}

// WriteMany upserts many points. Every point's time and every field name is
// resolved and validated before any row is inserted, and the whole batch is
// applied in a single transaction, so a write either lands in full or not at
// all (no partial batches on failure).
func (s *Store) WriteMany(ctx context.Context, series string, ps []Point) error {
	if len(ps) == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return err
	}

	// Resolve field names to ids once (cache across all points). seen tracks the
	// composite upsert key per batch so a duplicate (time, field) is rejected
	// here rather than relying on dialect-specific upsert behavior.
	fieldIDs := map[string]uint{}
	seen := map[recordKey]bool{}
	var rows []dbRecord
	for i, p := range ps {
		if p.Time.IsZero() {
			return fmt.Errorf("point %d: time cannot be zero", i)
		}
		if err := ensureUTC(p.Time); err != nil {
			return fmt.Errorf("point %d: %w", i, err)
		}
		if len(p.Values) == 0 {
			return fmt.Errorf("point %d: no values", i)
		}
		ms := p.Time.UTC().UnixMilli() // matches the stored millisecond key
		for name, val := range p.Values {
			if math.IsNaN(val) || math.IsInf(val, 0) {
				return fmt.Errorf("point %d field %q: value must be finite, got %v", i, name, val)
			}
			fid, ok := fieldIDs[name]
			if !ok {
				fid, err = s.fieldID(ctx, sid, name)
				if err != nil {
					return err
				}
				fieldIDs[name] = fid
			}
			k := recordKey{ms: ms, fid: fid}
			if seen[k] {
				return fmt.Errorf("point %d field %q: duplicate (time, field) in batch", i, name)
			}
			seen[k] = true
			rows = append(rows, dbRecord{
				SeriesID: sid,
				FieldID:  fid,
				Time:     unixMilli(p.Time),
				Value:    val,
			})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	// Conflict columns are listed in PK order (series_id, time, field_id) so the
	// upsert target matches the composite primary key on every dialect.
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "series_id"}, {Name: "time"}, {Name: "field_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"value"}),
		}).CreateInBatches(&rows, 500).Error
	})
}

// Range returns points in [start, end], pivoting records that share an exact
// timestamp into one Point. Returned in ascending time order. Pass a zero
// time.Time for an unbounded start or end.
func (s *Store) Range(ctx context.Context, series string, start, end time.Time) ([]Point, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(start); err != nil {
		return nil, err
	}
	if err := ensureUTC(end); err != nil {
		return nil, err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return nil, err
	}
	names, err := s.fieldNames(ctx, sid)
	if err != nil {
		return nil, err
	}

	var recs []dbRecord
	q := s.db.WithContext(ctx).Where("series_id = ?", sid)
	if !start.IsZero() {
		q = q.Where("time >= ?", unixMilli(start))
	}
	if !end.IsZero() {
		q = q.Where("time <= ?", unixMilli(end))
	}
	if err := q.Order("time ASC, field_id ASC").Find(&recs).Error; err != nil {
		return nil, err
	}

	var out []Point
	var cur *Point
	for _, r := range recs {
		ts := r.Time.asTime()
		if cur == nil || !cur.Time.Equal(ts) {
			out = append(out, Point{Time: ts, Values: map[string]float64{}})
			cur = &out[len(out)-1]
		}
		cur.Values[names[r.FieldID]] = r.Value
	}
	return out, nil
}

// FieldRange returns one field's scalar samples in [start, end], time-ascending.
// Pass a zero time.Time for an unbounded start or end.
func (s *Store) FieldRange(ctx context.Context, series, field string, start, end time.Time) ([]Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(start); err != nil {
		return nil, err
	}
	if err := ensureUTC(end); err != nil {
		return nil, err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return nil, err
	}
	fid, err := s.fieldID(ctx, sid, field)
	if err != nil {
		return nil, err
	}
	var recs []dbRecord
	q := s.db.WithContext(ctx).Where("series_id = ? AND field_id = ?", sid, fid)
	if !start.IsZero() {
		q = q.Where("time >= ?", unixMilli(start))
	}
	if !end.IsZero() {
		q = q.Where("time <= ?", unixMilli(end))
	}
	if err := q.Order("time ASC").Find(&recs).Error; err != nil {
		return nil, err
	}
	out := make([]Sample, len(recs))
	for i, r := range recs {
		out[i] = Sample{Time: r.Time.asTime(), Value: r.Value}
	}
	return out, nil
}

// FieldAt returns the latest value of a field at or before t.
func (s *Store) FieldAt(ctx context.Context, series, field string, t time.Time) (float64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(t); err != nil {
		return 0, false, err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return 0, false, err
	}
	fid, err := s.fieldID(ctx, sid, field)
	if err != nil {
		return 0, false, err
	}
	var r dbRecord
	res := s.db.WithContext(ctx).Where("series_id = ? AND field_id = ? AND time <= ?", sid, fid, unixMilli(t)).
		Order("time DESC").Limit(1).Find(&r)
	if res.Error != nil {
		return 0, false, res.Error
	}
	if res.RowsAffected == 0 {
		return 0, false, nil
	}
	return r.Value, true, nil
}

// At returns an as-of snapshot: each field's latest value at or before t.
// The returned Point.Time is the query time t; per-field source timestamps are
// not kept. An unknown series returns ErrSeriesNotFound (mirroring Latest/FieldAt).
//
// The snapshot is best-effort per field: each field resolves to its own latest
// value <= t independently, so a Point may be partial when fields have divergent
// histories. The returned Coverage reports this: CoverageNone (no field resolved,
// empty Point), CoveragePartial (some defined fields missing), or CoverageFull
// (every defined field present). Callers that require a complete record reject
// anything but CoverageFull instead of re-deriving the field set themselves.
func (s *Store) At(ctx context.Context, series string, t time.Time) (Point, Coverage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(t); err != nil {
		return Point{}, CoverageNone, err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return Point{}, CoverageNone, err
	}
	names, err := s.fieldNames(ctx, sid)
	if err != nil {
		return Point{}, CoverageNone, err
	}

	// Portable "latest per field <= t": match rows whose time equals the per-field max <= t.
	// The table name comes from dbRecord.TableName() so this raw query follows a rename.
	tbl := dbRecord{}.TableName()
	var recs []dbRecord
	err = s.db.WithContext(ctx).Raw(fmt.Sprintf(`
		SELECT r.series_id, r.field_id, r.time, r.value
		FROM %[1]s r
		WHERE r.series_id = ? AND r.time <= ?
		  AND r.time = (
			SELECT MAX(r2.time) FROM %[1]s r2
			WHERE r2.series_id = r.series_id AND r2.field_id = r.field_id AND r2.time <= ?
		  )
	`, tbl), sid, unixMilli(t), unixMilli(t)).Scan(&recs).Error
	if err != nil {
		return Point{}, CoverageNone, err
	}
	if len(recs) == 0 {
		return Point{}, CoverageNone, nil
	}

	out := Point{Time: t, Values: map[string]float64{}}
	for _, r := range recs {
		out.Values[names[r.FieldID]] = r.Value
	}
	return out, coverage(len(out.Values), len(names)), nil
}

// Latest returns the point at the series' most recent timestamp, with its real
// time preserved (unlike At, which stamps the query time). An unknown series
// returns ErrSeriesNotFound. Reads only the newest timestamp's rows, not the
// whole series.
//
// The returned Coverage describes the snapshot: CoverageNone when the series
// holds no records (empty Point), CoveragePartial when the newest timestamp
// carries only some of the defined fields (fields whose own latest write is
// older do not appear), or CoverageFull when every defined field is present.
func (s *Store) Latest(ctx context.Context, series string) (Point, Coverage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return Point{}, CoverageNone, err
	}
	names, err := s.fieldNames(ctx, sid)
	if err != nil {
		return Point{}, CoverageNone, err
	}

	// Every field at the series' newest timestamp, read as one consistent snapshot:
	// the MAX(time) subquery and the field read are a single statement, so a
	// concurrent write inserting a newer point cannot land between picking the
	// timestamp and reading its fields (the two-query version's race). MAX over an
	// empty series is NULL, so "time = (NULL)" matches no rows → found=false.
	newest := s.db.WithContext(ctx).Model(&dbRecord{}).
		Select("MAX(time)").Where("series_id = ?", sid)
	var recs []dbRecord
	if err := s.db.WithContext(ctx).Where("series_id = ? AND time = (?)", sid, newest).
		Order("field_id ASC").Find(&recs).Error; err != nil {
		return Point{}, CoverageNone, err
	}
	if len(recs) == 0 {
		return Point{}, CoverageNone, nil
	}
	out := Point{Time: recs[0].Time.asTime(), Values: map[string]float64{}}
	for _, r := range recs {
		out.Values[names[r.FieldID]] = r.Value
	}
	return out, coverage(len(out.Values), len(names)), nil
}

// LatestField returns the newest (time, value) for one field. found is false
// when the field has no samples. Unknown series/field return ErrSeriesNotFound /
// ErrFieldNotFound. Reads a single row.
func (s *Store) LatestField(ctx context.Context, series, field string) (Sample, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return Sample{}, false, err
	}
	fid, err := s.fieldID(ctx, sid, field)
	if err != nil {
		return Sample{}, false, err
	}
	var r dbRecord
	res := s.db.WithContext(ctx).Where("series_id = ? AND field_id = ?", sid, fid).
		Order("time DESC").Limit(1).Find(&r)
	if res.Error != nil {
		return Sample{}, false, res.Error
	}
	if res.RowsAffected == 0 {
		return Sample{}, false, nil
	}
	return Sample{Time: r.Time.asTime(), Value: r.Value}, true, nil
}

// Count returns the number of distinct timestamps (points) in the series,
// independent of how many fields each point carries. An unknown series returns
// ErrSeriesNotFound. Counts server-side rather than loading rows.
func (s *Store) Count(ctx context.Context, series string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return 0, err
	}
	var n int64
	if err := s.db.WithContext(ctx).Model(&dbRecord{}).
		Where("series_id = ?", sid).
		Distinct("time").Count(&n).Error; err != nil {
		return 0, err
	}
	return int(n), nil
}

// CountAll returns the number of distinct timestamps (points) per series, keyed
// by series name. With no options it covers every series; each MatchLabel option
// restricts the set, ANDing together (same semantics as ListSeries). Every
// matched series appears in the result, including those with no records (count
// 0). It runs a single GROUP BY instead of one Count per series, so it avoids the
// per-series query fan-out when summarizing many series at once.
func (s *Store) CountAll(ctx context.Context, opts ...ListOption) (map[string]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var f listFilter
	for _, opt := range opts {
		opt(&f)
	}

	// Resolve the matching series first, so a series with no records is still
	// reported (count 0) and the result can be keyed by name.
	q := s.db.WithContext(ctx).Model(&dbSeries{})
	if len(f.labels) > 0 {
		matchIDs, err := s.matchingSeriesIDs(ctx, f.labels)
		if err != nil {
			return nil, err
		}
		if len(matchIDs) == 0 {
			return map[string]int{}, nil
		}
		q = q.Where("id IN ?", matchIDs)
	}
	var rows []dbSeries
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int, len(rows))
	if len(rows) == 0 {
		return out, nil
	}
	nameByID := make(map[uint]string, len(rows))
	ids := make([]uint, len(rows))
	for i, r := range rows {
		nameByID[r.ID] = r.Name
		ids[i] = r.ID
		out[r.Name] = 0
	}

	// One GROUP BY collapses what would otherwise be one Count per series.
	var counts []struct {
		SeriesID uint
		N        int
	}
	if err := s.db.WithContext(ctx).Model(&dbRecord{}).
		Select("series_id, COUNT(DISTINCT time) AS n").
		Where("series_id IN ?", ids).
		Group("series_id").
		Scan(&counts).Error; err != nil {
		return nil, err
	}
	for _, c := range counts {
		out[nameByID[c.SeriesID]] = c.N
	}
	return out, nil
}

// Delete removes all fields at exactly t for the series. It reports whether a
// record existed at t: deleted is false (with a nil error) when no row matched,
// so callers can distinguish a real delete from a no-op. An unknown series
// returns ErrSeriesNotFound.
func (s *Store) Delete(ctx context.Context, series string, t time.Time) (deleted bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(t); err != nil {
		return false, err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return false, err
	}
	res := s.db.WithContext(ctx).Where("series_id = ? AND time = ?", sid, unixMilli(t)).Delete(&dbRecord{})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// DeleteRange removes all records in [start, end] for the series. Pass a zero
// time.Time for an unbounded start or end.
func (s *Store) DeleteRange(ctx context.Context, series string, start, end time.Time) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ensureUTC(start); err != nil {
		return err
	}
	if err := ensureUTC(end); err != nil {
		return err
	}
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return err
	}
	q := s.db.WithContext(ctx).Where("series_id = ?", sid)
	if !start.IsZero() {
		q = q.Where("time >= ?", unixMilli(start))
	}
	if !end.IsZero() {
		q = q.Where("time <= ?", unixMilli(end))
	}
	return q.Delete(&dbRecord{}).Error
}
