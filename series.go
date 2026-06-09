package timeseries

import (
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
func (s *Store) DefineSeries(cfg Series) error {
	if cfg.Name == "" {
		return fmt.Errorf("series name cannot be empty")
	}
	if cfg.Precision <= 0 || cfg.Retention <= 0 {
		return fmt.Errorf("precision and retention must be positive")
	}
	if cfg.Precision < time.Second {
		return fmt.Errorf("precision must be at least 1 second, got %v", cfg.Precision)
	}
	seen := map[string]bool{}
	for _, f := range cfg.Fields {
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

	return s.db.Transaction(func(tx *gorm.DB) error {
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

		var existing []dbField
		if err := tx.Where("series_id = ?", ser.ID).Find(&existing).Error; err != nil {
			return err
		}
		want := map[string]Field{}
		for _, f := range cfg.Fields {
			want[f.Name] = f
		}
		existingByName := map[string]dbField{}
		for _, ef := range existing {
			existingByName[ef.Name] = ef
		}

		// Delete fields absent from cfg.Fields, cascading their records.
		for _, ef := range existing {
			if _, keep := want[ef.Name]; keep {
				continue
			}
			if err := tx.Where("series_id = ? AND field_id = ?", ser.ID, ef.ID).
				Delete(&dbRecord{}).Error; err != nil {
				return err
			}
			if err := tx.Delete(&dbField{}, ef.ID).Error; err != nil {
				return err
			}
		}

		// Create new fields; update aggregate on existing ones.
		for _, f := range cfg.Fields {
			if ef, ok := existingByName[f.Name]; ok {
				if ef.AggregateFn != f.Aggregate {
					if err := tx.Model(&dbField{}).Where("id = ?", ef.ID).
						Update("aggregate_fn", f.Aggregate).Error; err != nil {
						return err
					}
				}
				continue
			}
			if err := tx.Create(&dbField{SeriesId: ser.ID, Name: f.Name, AggregateFn: f.Aggregate}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// GetSeries returns a series (with its fields) by name.
func (s *Store) GetSeries(name string) (Series, error) {
	var row dbSeries
	if err := s.db.Where("name = ?", name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return Series{}, fmt.Errorf("series %q not found", name)
		}
		return Series{}, err
	}
	fields, err := s.seriesFields(row.ID)
	if err != nil {
		return Series{}, err
	}
	return Series{Name: row.Name, Precision: row.Precision, Retention: row.Retention, Fields: fields}, nil
}

// ListSeries returns all series, each with its fields.
func (s *Store) ListSeries() ([]Series, error) {
	var rows []dbSeries
	if err := s.db.Order("name ASC").Find(&rows).Error; err != nil {
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
	if err := s.db.Where("series_id IN ?", ids).Order("name ASC").Find(&fields).Error; err != nil {
		return nil, err
	}
	for _, f := range fields {
		i := idx[f.SeriesId]
		out[i].Fields = append(out[i].Fields, Field{Name: f.Name, Aggregate: f.AggregateFn})
	}
	return out, nil
}

// DropSeries removes a series and all of its records (application-level cascade).
func (s *Store) DropSeries(name string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		var row dbSeries
		if err := tx.Where("name = ?", name).First(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("series %q not found", name)
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
func (s *Store) seriesID(name string) (uint, error) {
	var row dbSeries
	if err := s.db.Where("name = ?", name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, fmt.Errorf("series %q not found", name)
		}
		return 0, err
	}
	return row.ID, nil
}
