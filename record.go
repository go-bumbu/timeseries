package timeseries

import (
	"fmt"
	"time"

	"gorm.io/gorm/clause"
)

// dbRecord is the clustered fact table: one row per (series, field, time).
type dbRecord struct {
	SeriesId uint      `gorm:"primaryKey;autoIncrement:false"`
	FieldId  uint      `gorm:"primaryKey;autoIncrement:false"`
	Time     unixMilli `gorm:"primaryKey;autoIncrement:false"`
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
func (s *Store) Write(series string, p Point) error {
	return s.WriteMany(series, []Point{p})
}

// WriteMany upserts many points in one transaction. All points are validated first.
func (s *Store) WriteMany(series string, ps []Point) error {
	if len(ps) == 0 {
		return nil
	}
	sid, err := s.seriesID(series)
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
				fid, err = s.fieldID(name)
				if err != nil {
					return err
				}
				fieldIDs[name] = fid
			}
			rows = append(rows, dbRecord{
				SeriesId: sid,
				FieldId:  fid,
				Time:     unixMilli(p.Time),
				Value:    val,
			})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "series_id"}, {Name: "field_id"}, {Name: "time"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).CreateInBatches(&rows, 500).Error
}

// fieldNames returns an id->name map for all defined fields.
func (s *Store) fieldNames() (map[uint]string, error) {
	var rows []dbField
	if err := s.db.Find(&rows).Error; err != nil {
		return nil, err
	}
	m := make(map[uint]string, len(rows))
	for _, r := range rows {
		m[r.ID] = r.Name
	}
	return m, nil
}

// Range returns points in [start, end], pivoting records that share an exact
// timestamp into one Point. Returned in ascending time order.
func (s *Store) Range(series string, start, end time.Time) ([]Point, error) {
	sid, err := s.seriesID(series)
	if err != nil {
		return nil, err
	}
	names, err := s.fieldNames()
	if err != nil {
		return nil, err
	}

	var recs []dbRecord
	q := s.db.Where("series_id = ?", sid)
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
		cur.Values[names[r.FieldId]] = r.Value
	}
	return out, nil
}

// FieldRange returns one field's scalar samples in [start, end], time-ascending.
func (s *Store) FieldRange(series, field string, start, end time.Time) ([]Sample, error) {
	sid, err := s.seriesID(series)
	if err != nil {
		return nil, err
	}
	fid, err := s.fieldID(field)
	if err != nil {
		return nil, err
	}
	var recs []dbRecord
	q := s.db.Where("series_id = ? AND field_id = ?", sid, fid)
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
func (s *Store) FieldAt(series, field string, t time.Time) (float64, bool, error) {
	sid, err := s.seriesID(series)
	if err != nil {
		return 0, false, err
	}
	fid, err := s.fieldID(field)
	if err != nil {
		return 0, false, err
	}
	var r dbRecord
	res := s.db.Where("series_id = ? AND field_id = ? AND time <= ?", sid, fid, unixMilli(t)).
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
func (s *Store) At(series string, t time.Time) (Point, error) {
	sid, err := s.seriesID(series)
	if err != nil {
		return Point{}, err
	}
	names, err := s.fieldNames()
	if err != nil {
		return Point{}, err
	}

	// Portable "latest per field <= t": match rows whose time equals the per-field max <= t.
	var recs []dbRecord
	err = s.db.Raw(`
		SELECT r.series_id, r.field_id, r.time, r.value
		FROM records r
		WHERE r.series_id = ? AND r.time <= ?
		  AND r.time = (
			SELECT MAX(r2.time) FROM records r2
			WHERE r2.series_id = r.series_id AND r2.field_id = r.field_id AND r2.time <= ?
		  )
	`, sid, unixMilli(t), unixMilli(t)).Scan(&recs).Error
	if err != nil {
		return Point{}, err
	}

	out := Point{Time: t, Values: map[string]float64{}}
	for _, r := range recs {
		out.Values[names[r.FieldId]] = r.Value
	}
	return out, nil
}

// Delete removes all fields at exactly t for the series.
func (s *Store) Delete(series string, t time.Time) error {
	sid, err := s.seriesID(series)
	if err != nil {
		return err
	}
	return s.db.Where("series_id = ? AND time = ?", sid, unixMilli(t)).Delete(&dbRecord{}).Error
}

// DeleteRange removes all records in [start, end] for the series.
func (s *Store) DeleteRange(series string, start, end time.Time) error {
	sid, err := s.seriesID(series)
	if err != nil {
		return err
	}
	q := s.db.Where("series_id = ?", sid)
	if !start.IsZero() {
		q = q.Where("time >= ?", unixMilli(start))
	}
	if !end.IsZero() {
		q = q.Where("time <= ?", unixMilli(end))
	}
	return q.Delete(&dbRecord{}).Error
}
