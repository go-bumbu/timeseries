package timeseries

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// dbSeriesLabel is a per-series key/value label. A label belongs to exactly one
// series; (series_id, label_key) is unique. Integrity is application-level (no FK).
// Columns are named label_key/label_value to dodge reserved words (KEY/VALUE)
// across MySQL and PostgreSQL.
type dbSeriesLabel struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	SeriesID uint   `gorm:"column:series_id;not null;uniqueIndex:idx_series_label,priority:1"`
	Key      string `gorm:"column:label_key;not null;size:128;uniqueIndex:idx_series_label,priority:2;index:idx_label_kv,priority:1"`
	Value    string `gorm:"column:label_value;not null;size:255;index:idx_label_kv,priority:2"`
}

func (dbSeriesLabel) TableName() string { return "series_labels" }

// seriesLabels returns one series' labels as a map, or nil when it has none.
func (s *Store) seriesLabels(ctx context.Context, seriesID uint) (map[string]string, error) {
	var rows []dbSeriesLabel
	if err := s.db.WithContext(ctx).Where("series_id = ?", seriesID).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r.Key] = r.Value
	}
	return m, nil
}

// validateLabels rejects empty keys. Empty values are allowed.
func validateLabels(labels map[string]string) error {
	for k := range labels {
		if k == "" {
			return fmt.Errorf("label key cannot be empty")
		}
	}
	return nil
}

// sameLabelSet reports whether stored matches want exactly (same keys + values).
func sameLabelSet(stored, want map[string]string) bool {
	if len(stored) != len(want) {
		return false
	}
	for k, v := range want {
		if sv, ok := stored[k]; !ok || sv != v {
			return false
		}
	}
	return true
}

// listFilter accumulates ListSeries options.
type listFilter struct {
	labels map[string]string
}

// ListOption configures ListSeries.
type ListOption func(*listFilter)

// MatchLabel restricts ListSeries to series whose label `key` equals `value`.
// Multiple MatchLabel options AND together. Exact match only.
func MatchLabel(key, value string) ListOption {
	return func(f *listFilter) {
		if f.labels == nil {
			f.labels = make(map[string]string)
		}
		f.labels[key] = value
	}
}

// matchingSeriesIDs returns the ids of series carrying every (key,value) in
// labels (AND). It intersects the per-matcher id sets in Go; each matcher query
// uses the (label_key,label_value) index. labels must be non-empty.
func (s *Store) matchingSeriesIDs(ctx context.Context, labels map[string]string) ([]uint, error) {
	var result map[uint]bool
	for k, v := range labels {
		var ids []uint
		if err := s.db.WithContext(ctx).Model(&dbSeriesLabel{}).
			Where("label_key = ? AND label_value = ?", k, v).
			Pluck("series_id", &ids).Error; err != nil {
			return nil, err
		}
		set := make(map[uint]bool, len(ids))
		for _, id := range ids {
			set[id] = true
		}
		if result == nil {
			result = set
			continue
		}
		for id := range result {
			if !set[id] {
				delete(result, id)
			}
		}
	}
	out := make([]uint, 0, len(result))
	for id := range result {
		out = append(out, id)
	}
	return out, nil
}

// syncSeriesLabels declaratively reconciles the DB labels for seriesID against
// want: keys absent from want are deleted, new keys inserted, changed values
// updated. Mirrors syncSeriesFields. Must run inside the DefineSeries tx.
func (s *Store) syncSeriesLabels(tx *gorm.DB, seriesID uint, want map[string]string) error {
	var existing []dbSeriesLabel
	if err := tx.Where("series_id = ?", seriesID).Find(&existing).Error; err != nil {
		return err
	}
	existingByKey := make(map[string]dbSeriesLabel, len(existing))
	for _, el := range existing {
		existingByKey[el.Key] = el
	}
	for _, el := range existing {
		if _, keep := want[el.Key]; keep {
			continue
		}
		if err := tx.Delete(&dbSeriesLabel{}, el.ID).Error; err != nil {
			return err
		}
	}
	for k, v := range want {
		el, exists := existingByKey[k]
		if !exists {
			if err := tx.Create(&dbSeriesLabel{SeriesID: seriesID, Key: k, Value: v}).Error; err != nil {
				return err
			}
			continue
		}
		if el.Value != v {
			if err := tx.Model(&dbSeriesLabel{}).Where("id = ?", el.ID).Update("label_value", v).Error; err != nil {
				return err
			}
		}
	}
	return nil
}
