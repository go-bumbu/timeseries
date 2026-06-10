package timeseries

import (
	"context"
	"fmt"
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
type Point struct {
	Time   time.Time
	Values map[string]float64
}

// Sample is a single (time, value) pair for one field.
type Sample struct {
	Time  time.Time
	Value float64
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

	// Resolve field names to ids once (cache across all points).
	fieldIDs := map[string]uint{}
	var rows []dbRecord
	for i, p := range ps {
		if p.Time.IsZero() {
			return fmt.Errorf("point %d: time cannot be zero", i)
		}
		for name, val := range p.Values {
			fid, ok := fieldIDs[name]
			if !ok {
				fid, err = s.fieldID(ctx, sid, name)
				if err != nil {
					return err
				}
				fieldIDs[name] = fid
			}
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

// Move atomically replaces the record at oldTime with p: it deletes every field
// stored at oldTime and upserts p (at p.Time) inside a single transaction, so a
// crash can never leave the old timestamp removed without the new one written —
// the hazard a separate Delete + Write pair has. When oldTime equals p.Time it
// is a clean replace at that timestamp. A zero oldTime skips the delete, making
// it a plain upsert. Like Write, it takes the shared lock and rejects a zero
// p.Time.
func (s *Store) Move(ctx context.Context, series string, oldTime time.Time, p Point) error {
	if p.Time.IsZero() {
		return fmt.Errorf("point time cannot be zero")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return err
	}

	// Resolve and validate every field id before opening the transaction, so an
	// unknown field aborts without having deleted anything.
	rows := make([]dbRecord, 0, len(p.Values))
	for name, val := range p.Values {
		fid, err := s.fieldID(ctx, sid, name)
		if err != nil {
			return err
		}
		rows = append(rows, dbRecord{SeriesID: sid, FieldID: fid, Time: unixMilli(p.Time), Value: val})
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !oldTime.IsZero() {
			if err := tx.Where("series_id = ? AND time = ?", sid, unixMilli(oldTime)).Delete(&dbRecord{}).Error; err != nil {
				return err
			}
		}
		if len(rows) == 0 {
			return nil
		}
		// Conflict columns in PK order, matching WriteMany.
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
// The returned Point.Time is the query time t; per-field source timestamps are not kept.
func (s *Store) At(ctx context.Context, series string, t time.Time) (Point, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return Point{}, err
	}
	names, err := s.fieldNames(ctx, sid)
	if err != nil {
		return Point{}, err
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
		return Point{}, err
	}

	out := Point{Time: t, Values: map[string]float64{}}
	for _, r := range recs {
		out.Values[names[r.FieldID]] = r.Value
	}
	return out, nil
}

// Latest returns the point at the series' most recent timestamp, with its real
// time preserved (unlike At, which stamps the query time). found is false when
// the series exists but holds no records. An unknown series returns
// ErrSeriesNotFound. Reads only the newest timestamp's rows, not the whole series.
func (s *Store) Latest(ctx context.Context, series string) (Point, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return Point{}, false, err
	}
	names, err := s.fieldNames(ctx, sid)
	if err != nil {
		return Point{}, false, err
	}

	// Newest timestamp for the series.
	var newest dbRecord
	res := s.db.WithContext(ctx).Where("series_id = ?", sid).Order("time DESC").Limit(1).Find(&newest)
	if res.Error != nil {
		return Point{}, false, res.Error
	}
	if res.RowsAffected == 0 {
		return Point{}, false, nil
	}

	// All fields stored at that timestamp.
	var recs []dbRecord
	if err := s.db.WithContext(ctx).Where("series_id = ? AND time = ?", sid, newest.Time).
		Order("field_id ASC").Find(&recs).Error; err != nil {
		return Point{}, false, err
	}
	out := Point{Time: newest.Time.asTime(), Values: map[string]float64{}}
	for _, r := range recs {
		out.Values[names[r.FieldID]] = r.Value
	}
	return out, true, nil
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

// Delete removes all fields at exactly t for the series.
func (s *Store) Delete(ctx context.Context, series string, t time.Time) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sid, err := s.seriesID(ctx, series)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Where("series_id = ? AND time = ?", sid, unixMilli(t)).Delete(&dbRecord{}).Error
}

// DeleteRange removes all records in [start, end] for the series. Pass a zero
// time.Time for an unbounded start or end.
func (s *Store) DeleteRange(ctx context.Context, series string, start, end time.Time) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
