package timeseries

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbSeries is the stored series definition.
type dbSeries struct {
	ID        uint          `gorm:"primaryKey;autoIncrement"`
	Name      string        `gorm:"uniqueIndex;not null;size:255"`
	Precision time.Duration `gorm:"not null"`
	Retention time.Duration `gorm:"not null"`
}

func (dbSeries) TableName() string { return "series" }

// Series is a named time series with its fields, precision and retention.
type Series struct {
	Name      string
	Precision time.Duration
	Retention time.Duration
	Fields    []Field
}

// DefineSeries creates or updates a series by name and declaratively syncs its
// fields: fields absent from cfg.Fields are deleted (cascading their records),
// new fields are created, and existing fields' aggregates are updated. All in
// one transaction.
//
// DefineSeries takes the Store lock exclusively: it cannot run concurrently
// with writes, reads, or Maintain on the same Store, which is what keeps a
// field deletion from racing a concurrent write into orphan records.
func (s *Store) DefineSeries(ctx context.Context, cfg Series) error {
	if cfg.Name == "" {
		return fmt.Errorf("series name cannot be empty")
	}
	if cfg.Precision <= 0 || cfg.Retention <= 0 {
		return fmt.Errorf("precision and retention must be positive")
	}
	if cfg.Precision < time.Second {
		return fmt.Errorf("precision must be at least 1 second, got %v", cfg.Precision)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateFields(cfg.Fields); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := dbSeries{Name: cfg.Name, Precision: cfg.Precision, Retention: cfg.Retention}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "name"}},
			DoUpdates: clause.AssignmentColumns([]string{"precision", "retention"}),
		}).Create(&row).Error; err != nil {
			return err
		}
		// Re-read to get a reliable id across dialects (OnConflict update may not populate row.ID).
		var ser dbSeries
		if err := tx.Where("name = ?", cfg.Name).First(&ser).Error; err != nil {
			return err
		}
		return s.syncSeriesFields(tx, ser.ID, cfg.Fields)
	})
}

// validateFields checks field names are non-empty, unique, and use known
// aggregates. It reads s.aggregates without locking, so callers must hold s.mu
// (DefineSeries does). Do not call it outside the lock.
func (s *Store) validateFields(fields []Field) error {
	seen := map[string]bool{}
	for _, f := range fields {
		if f.Name == "" {
			return fmt.Errorf("field name cannot be empty")
		}
		if seen[f.Name] {
			return fmt.Errorf("duplicate field %q", f.Name)
		}
		seen[f.Name] = true
		if f.Aggregate != "" {
			if _, ok := s.aggregates[f.Aggregate]; !ok {
				return fmt.Errorf("unknown aggregate %q", f.Aggregate)
			}
		}
	}
	return nil
}

// syncSeriesFields declaratively reconciles the DB fields for seriesID against want.
func (s *Store) syncSeriesFields(tx *gorm.DB, seriesID uint, want []Field) error {
	var existing []dbField
	if err := tx.Where("series_id = ?", seriesID).Find(&existing).Error; err != nil {
		return err
	}
	wantByName := make(map[string]Field, len(want))
	for _, f := range want {
		wantByName[f.Name] = f
	}
	existingByName := make(map[string]dbField, len(existing))
	for _, ef := range existing {
		existingByName[ef.Name] = ef
	}
	if err := s.deleteAbsentFields(tx, seriesID, existing, wantByName); err != nil {
		return err
	}
	return s.upsertWantedFields(tx, seriesID, want, existingByName)
}

// deleteAbsentFields removes fields (and their records) not present in wantByName.
func (s *Store) deleteAbsentFields(tx *gorm.DB, seriesID uint, existing []dbField, wantByName map[string]Field) error {
	for _, ef := range existing {
		if _, keep := wantByName[ef.Name]; keep {
			continue
		}
		if err := tx.Where("series_id = ? AND field_id = ?", seriesID, ef.ID).Delete(&dbRecord{}).Error; err != nil {
			return err
		}
		if err := tx.Delete(&dbField{}, ef.ID).Error; err != nil {
			return err
		}
	}
	return nil
}

// upsertWantedFields creates new fields or updates the aggregate of existing ones.
func (s *Store) upsertWantedFields(tx *gorm.DB, seriesID uint, want []Field, existingByName map[string]dbField) error {
	for _, f := range want {
		ef, exists := existingByName[f.Name]
		if !exists {
			if err := tx.Create(&dbField{SeriesID: seriesID, Name: f.Name, AggregateFn: f.Aggregate}).Error; err != nil {
				return err
			}
			continue
		}
		if ef.AggregateFn != f.Aggregate {
			if err := tx.Model(&dbField{}).Where("id = ?", ef.ID).Update("aggregate_fn", f.Aggregate).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

// GetSeries returns a series (with its fields) by name.
func (s *Store) GetSeries(ctx context.Context, name string) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var row dbSeries
	if err := s.db.WithContext(ctx).Where("name = ?", name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return Series{}, fmt.Errorf("series %q: %w", name, ErrSeriesNotFound)
		}
		return Series{}, err
	}
	fields, err := s.seriesFields(ctx, row.ID)
	if err != nil {
		return Series{}, err
	}
	return Series{Name: row.Name, Precision: row.Precision, Retention: row.Retention, Fields: fields}, nil
}

// ListSeries returns all series, each with its fields.
func (s *Store) ListSeries(ctx context.Context) ([]Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var rows []dbSeries
	if err := s.db.WithContext(ctx).Order("name ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]uint, len(rows))
	idx := make(map[uint]int, len(rows))
	out := make([]Series, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
		idx[r.ID] = i
		out[i] = Series{Name: r.Name, Precision: r.Precision, Retention: r.Retention}
	}
	var fields []dbField
	if err := s.db.WithContext(ctx).Where("series_id IN ?", ids).Order("name ASC").Find(&fields).Error; err != nil {
		return nil, err
	}
	for _, f := range fields {
		i := idx[f.SeriesID]
		out[i].Fields = append(out[i].Fields, Field{Name: f.Name, Aggregate: f.AggregateFn})
	}
	return out, nil
}

// DropSeries removes a series and all of its records (application-level cascade).
// It takes the Store lock exclusively.
func (s *Store) DropSeries(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row dbSeries
		if err := tx.Where("name = ?", name).First(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("series %q: %w", name, ErrSeriesNotFound)
			}
			return err
		}
		if err := tx.Where("series_id = ?", row.ID).Delete(&dbRecord{}).Error; err != nil {
			return err
		}
		if err := tx.Where("series_id = ?", row.ID).Delete(&dbField{}).Error; err != nil {
			return err
		}
		return tx.Delete(&dbSeries{}, row.ID).Error
	})
}

// seriesID resolves a series name to its id; errors if undefined.
func (s *Store) seriesID(ctx context.Context, name string) (uint, error) {
	var row dbSeries
	if err := s.db.WithContext(ctx).Where("name = ?", name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, fmt.Errorf("series %q: %w", name, ErrSeriesNotFound)
		}
		return 0, err
	}
	return row.ID, nil
}
