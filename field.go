package timeseries

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// dbField is the per-series field dimension. A field belongs to exactly one
// series; (series_id, name) is unique. Integrity is application-level (no FK).
type dbField struct {
	ID          uint   `gorm:"primaryKey;autoIncrement"`
	SeriesID    uint   `gorm:"column:series_id;not null;uniqueIndex:idx_series_field,priority:1"`
	Name        string `gorm:"not null;size:64;uniqueIndex:idx_series_field,priority:2"`
	AggregateFn string `gorm:"not null;size:32"`
}

func (dbField) TableName() string { return "fields" }

// Field is a series-scoped measurement name with its bucket aggregation.
type Field struct {
	Name      string
	Aggregate string // AggLast, AggMax, ...; "" means no bucket reduction
}

// fieldID resolves a field name within a series to its id; errors if undefined.
func (s *Store) fieldID(ctx context.Context, seriesID uint, name string) (uint, error) {
	var f dbField
	if err := s.db.WithContext(ctx).Where("series_id = ? AND name = ?", seriesID, name).First(&f).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, fmt.Errorf("field %q: %w", name, ErrFieldNotFound)
		}
		return 0, err
	}
	return f.ID, nil
}

// fieldNames returns an id->name map for one series' fields.
func (s *Store) fieldNames(ctx context.Context, seriesID uint) (map[uint]string, error) {
	var rows []dbField
	if err := s.db.WithContext(ctx).Where("series_id = ?", seriesID).Find(&rows).Error; err != nil {
		return nil, err
	}
	m := make(map[uint]string, len(rows))
	for _, r := range rows {
		m[r.ID] = r.Name
	}
	return m, nil
}

// seriesFields returns one series' fields as API values, name-ascending.
func (s *Store) seriesFields(ctx context.Context, seriesID uint) ([]Field, error) {
	var rows []dbField
	if err := s.db.WithContext(ctx).Where("series_id = ?", seriesID).Order("name ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]Field, len(rows))
	for i, r := range rows {
		out[i] = Field{Name: r.Name, Aggregate: r.AggregateFn}
	}
	return out, nil
}
