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
	Labels    map[string]string // opaque key/value metadata; nil when none
}

// DefineSeries creates or updates a series by name and declaratively syncs its
// fields: fields absent from cfg.Fields are deleted (cascading their records),
// new fields are created, and existing fields' aggregates are updated. All in
// one transaction.
//
// When the stored definition already matches cfg exactly, DefineSeries is a
// no-op that takes only the read lock — it neither acquires the exclusive lock
// nor opens a write transaction. This keeps the common auto-register-before-
// write pattern cheap. Only an actual change (new series, or differing
// precision/retention/fields) escalates to the exclusive define-and-reconcile.
//
// On that escalation path DefineSeries takes the Store lock exclusively: it
// cannot run concurrently with writes, reads, or Maintain on the same Store,
// which is what keeps a field deletion from racing a concurrent write into
// orphan records.
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
	// Fast path: a define identical to what is already stored changes nothing,
	// so resolve it under the read lock and return without the exclusive lock or
	// a write transaction. Invalid configs (duplicate field names, unknown
	// aggregates) cannot match a stored definition, so they fall through to the
	// exclusive path below where validateFields rejects them.
	if unchanged, err := s.definitionUnchanged(ctx, cfg); err != nil {
		return err
	} else if unchanged {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateFields(cfg.Fields); err != nil {
		return err
	}
	if err := validateLabels(cfg.Labels); err != nil {
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
		if err := s.syncSeriesFields(tx, ser.ID, cfg.Fields); err != nil {
			return err
		}
		return s.syncSeriesLabels(tx, ser.ID, cfg.Labels)
	})
}

// definitionUnchanged reports whether the series named cfg.Name already exists
// with a definition identical to cfg (same precision, retention, and field
// set). It takes only the read lock, letting a redundant DefineSeries skip the
// exclusive lock and write transaction. A missing series, or any difference,
// reports false so the caller escalates to the full define-and-reconcile path.
func (s *Store) definitionUnchanged(ctx context.Context, cfg Series) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var row dbSeries
	if err := s.db.WithContext(ctx).Where("name = ?", cfg.Name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if row.Precision != cfg.Precision || row.Retention != cfg.Retention {
		return false, nil
	}
	fields, err := s.seriesFields(ctx, row.ID)
	if err != nil {
		return false, err
	}
	if !sameFieldSet(fields, cfg.Fields) {
		return false, nil
	}
	labels, err := s.seriesLabels(ctx, row.ID)
	if err != nil {
		return false, err
	}
	return sameLabelSet(labels, cfg.Labels), nil
}

// sameFieldSet reports whether want describes exactly the stored field set,
// matching on (name, aggregate) and ignoring order. A duplicate field name in
// want makes it not a clean match (false), so the caller escalates and
// validateFields rejects the invalid config.
func sameFieldSet(stored, want []Field) bool {
	if len(stored) != len(want) {
		return false
	}
	wantByName := make(map[string]string, len(want))
	for _, f := range want {
		wantByName[f.Name] = f.Aggregate
	}
	if len(wantByName) != len(want) {
		return false
	}
	for _, f := range stored {
		agg, ok := wantByName[f.Name]
		if !ok || agg != f.Aggregate {
			return false
		}
	}
	return true
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
	labels, err := s.seriesLabels(ctx, row.ID)
	if err != nil {
		return Series{}, err
	}
	return Series{Name: row.Name, Precision: row.Precision, Retention: row.Retention, Fields: fields, Labels: labels}, nil
}

// ListSeries returns series (each with its fields and labels). With no options
// it returns all series; each MatchLabel option restricts the result, ANDing
// together. Backward compatible: ListSeries(ctx) behaves as before.
func (s *Store) ListSeries(ctx context.Context, opts ...ListOption) ([]Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var f listFilter
	for _, opt := range opts {
		opt(&f)
	}

	q := s.db.WithContext(ctx).Order("name ASC")
	if len(f.labels) > 0 {
		matchIDs, err := s.matchingSeriesIDs(ctx, f.labels)
		if err != nil {
			return nil, err
		}
		if len(matchIDs) == 0 {
			return nil, nil
		}
		q = q.Where("id IN ?", matchIDs)
	}

	var rows []dbSeries
	if err := q.Find(&rows).Error; err != nil {
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
	for _, fl := range fields {
		i := idx[fl.SeriesID]
		out[i].Fields = append(out[i].Fields, Field{Name: fl.Name, Aggregate: fl.AggregateFn})
	}
	var labels []dbSeriesLabel
	if err := s.db.WithContext(ctx).Where("series_id IN ?", ids).Find(&labels).Error; err != nil {
		return nil, err
	}
	for _, lb := range labels {
		i := idx[lb.SeriesID]
		if out[i].Labels == nil {
			out[i].Labels = make(map[string]string)
		}
		out[i].Labels[lb.Key] = lb.Value
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
		if err := tx.Where("series_id = ?", row.ID).Delete(&dbSeriesLabel{}).Error; err != nil {
			return err
		}
		return tx.Delete(&dbSeries{}, row.ID).Error
	})
}

// Wipe removes every record, field, and series in one transaction under the
// exclusive lock — the bulk equivalent of DropSeries for all series at once.
// Deletes go through the model types, so the table names stay tied to their
// TableName methods rather than being hardcoded by callers.
func (s *Store) Wipe(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Order mirrors DropSeries: records reference fields, fields reference series.
		if err := tx.Where("1 = 1").Delete(&dbRecord{}).Error; err != nil {
			return err
		}
		if err := tx.Where("1 = 1").Delete(&dbField{}).Error; err != nil {
			return err
		}
		if err := tx.Where("1 = 1").Delete(&dbSeriesLabel{}).Error; err != nil {
			return err
		}
		return tx.Where("1 = 1").Delete(&dbSeries{}).Error
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
