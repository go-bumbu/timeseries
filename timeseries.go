package timeseries

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type Registry struct {
	db *gorm.DB
}

// NewRegistry creates a new dbTimeSeries instance
func NewRegistry(db *gorm.DB) (*Registry, error) {
	if err := db.AutoMigrate(&dbTimeSeries{}, &dbSamplingPolicy{}, &dbRecord{}); err != nil {
		return nil, err
	}
	return &Registry{db: db}, nil
}

// SamplingPolicy defines the retention and precision for a series.
type SamplingPolicy struct {
	Retention time.Duration
	Precision time.Duration
}

// TimeSeries represents one time series configuration
type TimeSeries struct {
	Name      string
	Retention SamplingPolicy // main ingestion retention policy
}

// dbTimeSeries represents one time series configuration
type dbTimeSeries struct {
	ID       uint               `gorm:"primaryKey;autoIncrement"`
	Name     string             `gorm:"uniqueIndex;not null;size:255"` // logical name, must be unique
	Policies []dbSamplingPolicy `gorm:"foreignKey:TimeSeriesID;constraint:OnDelete:CASCADE"`
}

func (dbTs *dbTimeSeries) mainPolicyID() uint {
	for _, p := range dbTs.Policies {
		if p.Name == mainPolicyName {
			return p.ID
		}
	}
	return 0
}

// dbSamplingPolicy defines a rollup/aggregation policy for a series
type dbSamplingPolicy struct {
	ID            uint          `gorm:"primaryKey"`
	TimeSeriesID  uint          `gorm:"not null;index:idx_series_policy,unique"`          // FK to dbTimeSeries.id
	Name          string        `gorm:"not null;index:idx_series_policy,unique;size:255"` // unique per series
	Precision     time.Duration `gorm:"not null"`
	Retention     time.Duration `gorm:"not null"`
	AggregationFn string        `gorm:"not null;size:32"`
}

const aggrDBDefault = "avg" // only aggregation supported in DB; nil AggregationFn uses AVG

const mainPolicyName = "main"

// RegisterSeries inserts or updates a series.
// A main policy (Retention with positive retention duration) is required.
func (ts *Registry) RegisterSeries(series TimeSeries) error {
	if series.Retention.Retention <= 0 || series.Retention.Precision <= 0 {
		return fmt.Errorf("main policy is required: retention and presicions must be set")
	}
	return ts.db.Transaction(func(tx *gorm.DB) error {
		existing, err := findOrCreateSeries(tx, series.Name)
		if err != nil {
			return err
		}
		existingPolicies, err := loadPolicies(tx, existing.ID)
		if err != nil {
			return err
		}
		existingMap := policiesByName(existingPolicies)
		if err := upsertMainPolicy(tx, existing.ID, existingMap, series.Retention); err != nil {
			return err
		}
		return deleteNonMainPolicies(tx, existingPolicies)
	})
}

func loadPolicies(tx *gorm.DB, seriesID uint) ([]dbSamplingPolicy, error) {
	var out []dbSamplingPolicy
	if err := tx.Where("time_series_id = ?", seriesID).Find(&out).Error; err != nil {
		return nil, fmt.Errorf("load existing policies: %w", err)
	}
	return out, nil
}

func policiesByName(policies []dbSamplingPolicy) map[string]dbSamplingPolicy {
	m := make(map[string]dbSamplingPolicy, len(policies))
	for _, p := range policies {
		m[p.Name] = p
	}
	return m
}

func upsertMainPolicy(tx *gorm.DB, seriesID uint, existingMap map[string]dbSamplingPolicy, data SamplingPolicy) error {
	existing, ok := existingMap[mainPolicyName]
	if ok {
		if existing.Precision == data.Precision && existing.Retention == data.Retention {
			return nil
		}
		existing.Precision = data.Precision
		existing.Retention = data.Retention
		existing.AggregationFn = aggrDBDefault
		if err := tx.Save(&existing).Error; err != nil {
			return fmt.Errorf("update main policy: %w", err)
		}
		return nil
	}
	if err := tx.Create(&dbSamplingPolicy{
		Name:          mainPolicyName,
		TimeSeriesID:  seriesID,
		Precision:     data.Precision,
		Retention:     data.Retention,
		AggregationFn: aggrDBDefault,
	}).Error; err != nil {
		return fmt.Errorf("create main policy: %w", err)
	}
	return nil
}

func deleteNonMainPolicies(tx *gorm.DB, policies []dbSamplingPolicy) error {
	for _, old := range policies {
		if old.Name == mainPolicyName {
			continue
		}
		if err := tx.Delete(&old).Error; err != nil {
			return fmt.Errorf("delete policy %s: %w", old.Name, err)
		}
	}
	return nil
}

func findOrCreateSeries(tx *gorm.DB, name string) (dbTimeSeries, error) {
	var existing dbTimeSeries
	err := tx.Where("name = ?", name).First(&existing).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		newSeries := dbTimeSeries{Name: name}
		if err := tx.Create(&newSeries).Error; err != nil {
			return dbTimeSeries{}, fmt.Errorf("create series: %w", err)
		}
		return newSeries, nil
	case err != nil:
		return dbTimeSeries{}, fmt.Errorf("find series: %w", err)
	default:
		return existing, nil
	}
}

// ListSeries returns all series with their retention policy
func (ts *Registry) ListSeries() ([]TimeSeries, error) {
	var dbSeries []dbTimeSeries
	if err := ts.db.Preload("Policies").Find(&dbSeries).Error; err != nil {
		return nil, err
	}

	result := make([]TimeSeries, len(dbSeries))
	for i, s := range dbSeries {
		out := TimeSeries{Name: s.Name}
		for _, p := range s.Policies {
			if p.Name == mainPolicyName {
				out.Retention = SamplingPolicy{Precision: p.Precision, Retention: p.Retention}
				break
			}
		}
		result[i] = out
	}

	return result, nil
}

// GetSeries loads a series with its retention policy
func (ts *Registry) GetSeries(name string) (TimeSeries, error) {
	var series dbTimeSeries
	err := ts.db.Preload("Policies").Where("name = ?", name).First(&series).Error
	if err != nil {
		return TimeSeries{}, err
	}
	ret := TimeSeries{Name: series.Name}
	for _, p := range series.Policies {
		if p.Name == mainPolicyName {
			ret.Retention = SamplingPolicy{Precision: p.Precision, Retention: p.Retention}
			break
		}
	}
	return ret, err
}

// Maintenance runs background tasks (e.g. retention cleanup) for all series.
func (ts *Registry) Maintenance(ctx context.Context) error {
	var dbSeries []dbTimeSeries
	if err := ts.db.Preload("Policies").Find(&dbSeries).Error; err != nil {
		return err
	}
	for _, item := range dbSeries {
		if err := ts.cleanOneSeries(ctx, item); err != nil {
			return fmt.Errorf("unable to clean series %s: %w", item.Name, err)
		}
	}
	return nil
}

// cleanOneSeries deletes records older than the retention period for each policy of the series.
func (ts *Registry) cleanOneSeries(ctx context.Context, series dbTimeSeries) error {
	now := time.Now()
	for _, p := range series.Policies {
		cutoff := now.Add(-p.Retention)
		if err := ts.db.WithContext(ctx).Where("sampling_id = ? AND time < ?", p.ID, cutoff).Delete(&dbRecord{}).Error; err != nil {
			return fmt.Errorf("clean policy %s: %w", p.Name, err)
		}
	}
	return nil
}
