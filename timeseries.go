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

// AggregationFunc aggregates a slice of values into a single value. If nil, downsampling uses the DB AVG function.
type AggregationFunc func([]float64) float64

// SamplingPolicy defines a rollup/aggregation policy for a series
type SamplingPolicy struct {
	Retention     time.Duration
	Precision     time.Duration
	AggregationFn AggregationFunc
}

// TimeSeries represents one time series configuration
type TimeSeries struct {
	Name         string
	Retention    SamplingPolicy   // main ingestion retention policy
	DownSampling []SamplingPolicy // additional downsampling policies
}

// dbTimeSeries represents one time series configuration
type dbTimeSeries struct {
	ID       uint               `gorm:"primaryKey;autoIncrement"`
	Name     string             `gorm:"uniqueIndex;not null"` // logical name, must be unique
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
	TimeSeriesID  uint          `gorm:"not null;index:idx_series_policy,unique"` // FK to dbTimeSeries.id
	Name          string        `gorm:"not null;index:idx_series_policy,unique"` // unique per series
	Precision     time.Duration `gorm:"not null"`
	Retention     time.Duration `gorm:"not null"`
	AggregationFn string        `gorm:"not null"`
}

const aggrDBDefault = "avg" // only aggregation supported in DB; nil AggregationFn uses AVG

// samplingPolicyName used internally to generate a unique identifying name for sampling policies
func samplingPolicyName(retention, precision time.Duration) string {
	return fmt.Sprintf("%s_%s_%s", retention.String(), precision.String(), aggrDBDefault)
}

const mainPolicyName = "main"

// RegisterSeries inserts or updates a series.
// If it exists, all DownSamplingPolicies are replaced.
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

		// Load existing policies
		var existingPolicies []dbSamplingPolicy
		if err := tx.Where("time_series_id = ?", existing.ID).Find(&existingPolicies).Error; err != nil {
			return fmt.Errorf("load existing policies: %w", err)
		}

		existingMap := make(map[string]dbSamplingPolicy)
		for _, p := range existingPolicies {
			existingMap[p.Name] = p
		}
		seen := make(map[string]bool)

		mainPolicyData := series.Retention
		if mainPolicyData.Retention > 0 {
			for _, p := range series.DownSampling {
				if p.Precision > mainPolicyData.Retention {
					return fmt.Errorf("downsampling policy precision %s exceeds main retention %s: main must retain at least one full bucket to derive downsampled data",
						p.Precision, mainPolicyData.Retention)
				}
			}
		}

		// Handle main retention policy
		seen[mainPolicyName] = true
		if existingMain, ok := existingMap[mainPolicyName]; ok {
			// Update existing main policy if changed
			if existingMain.Precision != mainPolicyData.Precision ||
				existingMain.Retention != mainPolicyData.Retention {
				existingMain.Precision = mainPolicyData.Precision
				existingMain.Retention = mainPolicyData.Retention
				existingMain.AggregationFn = aggrDBDefault
				if err := tx.Save(&existingMain).Error; err != nil {
					return fmt.Errorf("update main policy: %w", err)
				}
			}
		} else {
			// Create new main policy
			mainPolicy := dbSamplingPolicy{
				Name:          mainPolicyName,
				TimeSeriesID:  existing.ID,
				Precision:     mainPolicyData.Precision,
				Retention:     mainPolicyData.Retention,
				AggregationFn: aggrDBDefault,
			}
			if err := tx.Create(&mainPolicy).Error; err != nil {
				return fmt.Errorf("create main policy: %w", err)
			}
		}

		// Handle downsampling policies, main policy is not habdled here
		if err := upsertPolicies(tx, existing.ID, existingMap, seen, series.DownSampling); err != nil {
			return err
		}

		// Delete old policies not present anymore
		for _, old := range existingPolicies {
			if !seen[old.Name] {
				if err := tx.Delete(&old).Error; err != nil {
					return fmt.Errorf("delete policy %s: %w", old.Name, err)
				}
			}
		}
		return nil
	})
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

func upsertPolicies(tx *gorm.DB, seriesID uint, existingMap map[string]dbSamplingPolicy, seen map[string]bool, sampling []SamplingPolicy) error {
	for _, p := range sampling {
		name := samplingPolicyName(p.Retention, p.Precision)
		seen[name] = true

		if existing, ok := existingMap[name]; ok {
			if existing.Precision == p.Precision && existing.Retention == p.Retention {
				continue // no change
			}

			existing.Precision = p.Precision
			existing.Retention = p.Retention
			existing.AggregationFn = aggrDBDefault
			if err := tx.Save(&existing).Error; err != nil {
				return fmt.Errorf("update policy %s: %w", name, err)
			}
			continue
		}

		newPolicy := dbSamplingPolicy{
			Name:          name,
			TimeSeriesID:  seriesID,
			Precision:     p.Precision,
			Retention:     p.Retention,
			AggregationFn: aggrDBDefault,
		}
		if err := tx.Create(&newPolicy).Error; err != nil {
			return fmt.Errorf("create policy %s: %w", name, err)
		}
	}
	return nil
}

// ListSeries returns all series with their downsampling policies
func (ts *Registry) ListSeries() ([]TimeSeries, error) {
	var dbSeries []dbTimeSeries
	if err := ts.db.Preload("Policies").Find(&dbSeries).Error; err != nil {
		return nil, err
	}

	result := make([]TimeSeries, len(dbSeries))
	for i, s := range dbSeries {
		out := TimeSeries{
			Name: s.Name,
		}

		// Separate main retention policy from downsampling policies
		var downsampling []SamplingPolicy
		for _, p := range s.Policies {
			policy := SamplingPolicy{
				Precision:     p.Precision,
				Retention:     p.Retention,
				AggregationFn: nil,
			}
			if p.Name == mainPolicyName {
				out.Retention = policy
			} else {
				downsampling = append(downsampling, policy)
			}
		}
		out.DownSampling = downsampling
		result[i] = out
	}

	return result, nil
}

// GetSeries loads a series with its DownSampling policies preloaded
func (ts *Registry) GetSeries(name string) (TimeSeries, error) {
	var series dbTimeSeries
	err := ts.db.Preload("Policies").Where("name = ?", name).First(&series).Error
	if err != nil {
		return TimeSeries{}, err
	}
	ret := TimeSeries{
		Name: series.Name,
	}

	// Separate main retention policy from downsampling policies
	var downsampling []SamplingPolicy
	for _, p := range series.Policies {
		policy := SamplingPolicy{
			Precision:     p.Precision,
			Retention:     p.Retention,
			AggregationFn: nil,
		}
		if p.Name == mainPolicyName {
			ret.Retention = policy
		} else {
			downsampling = append(downsampling, policy)
		}
	}
	ret.DownSampling = downsampling
	return ret, err
}

// Maintenance function to run all background tasks related to
// clean up and down-sampling
func (ts *Registry) Maintenance(ctx context.Context) error {
	// Do cleanup of old series

	var dbSeries []dbTimeSeries
	if err := ts.db.Preload("Policies").Find(&dbSeries).Error; err != nil {
		return err
	}

	for _, item := range dbSeries {
		if err := ts.cleanOneSeries(ctx, item); err != nil {
			return fmt.Errorf("unable to clean series %s: %w", item.Name, err)
		}
	}

	for _, item := range dbSeries {
		if err := ts.downSampleOne(ctx, item); err != nil {
			return fmt.Errorf("unable to downsample series %s: %w", item.Name, err)
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

// downSampleOne aggregates main-policy records into each downsampling policy by time buckets (Precision).
// For each bucket that has main data and no existing downsampled value, it uses the DB AVG function and inserts one record.
func (ts *Registry) downSampleOne(ctx context.Context, series dbTimeSeries) error {

	mainID := series.mainPolicyID()
	if mainID == 0 {
		return fmt.Errorf("no main policy found, ID 0")
	}

	for _, p := range series.Policies {
		if p.Name == mainPolicyName {
			continue
		}
		precision := p.Precision
		if precision <= 0 {
			continue
		}

		var minT, maxT *time.Time
		ts.db.WithContext(ctx).Model(&dbRecord{}).Where("sampling_id = ?", mainID).Select("MIN(time)").Scan(&minT)
		ts.db.WithContext(ctx).Model(&dbRecord{}).Where("sampling_id = ?", mainID).Select("MAX(time)").Scan(&maxT)
		if minT == nil || maxT == nil {
			continue
		}
		bucketStart := minT.Truncate(precision)
		for t := bucketStart; !t.After(*maxT); t = t.Add(precision) {
			bucketEnd := t.Add(precision)
			var exists int64
			if err := ts.db.WithContext(ctx).Model(&dbRecord{}).Where("sampling_id = ? AND time = ?", p.ID, t).Count(&exists).Error; err != nil {
				return fmt.Errorf("check existing downsampled: %w", err)
			}
			if exists > 0 {
				continue
			}

			var count int64
			if err := ts.db.WithContext(ctx).Model(&dbRecord{}).Where("sampling_id = ? AND time >= ? AND time < ?", mainID, t, bucketEnd).Count(&count).Error; err != nil {
				return fmt.Errorf("count main records: %w", err)
			}
			if count == 0 {
				continue
			}

			var value float64
			ts.db.WithContext(ctx).Model(&dbRecord{}).Select("AVG(value)").Where("sampling_id = ? AND time >= ? AND time < ?", mainID, t, bucketEnd).Scan(&value)

			if err := ts.db.WithContext(ctx).Create(&dbRecord{SamplingId: p.ID, Time: t, Value: value}).Error; err != nil {
				return fmt.Errorf("insert downsampled record: %w", err)
			}
		}
	}
	return nil
}
