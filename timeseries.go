package timeseries

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type Registry struct {
	db           *gorm.DB
	aggregateFns map[string]AggregateFn
}

// AggregateFn takes a slice of values in a time bucket and returns a single aggregated value.
type AggregateFn func(values []float64) float64

const AggregateAVG = "avg"

// NewRegistry creates a new Registry and registers the built-in AVG aggregate.
func NewRegistry(db *gorm.DB) (*Registry, error) {
	if err := db.AutoMigrate(&dbTimeSeries{}, &dbSamplingPolicy{}, &dbRecord{}); err != nil {
		return nil, err
	}
	reg := &Registry{db: db, aggregateFns: make(map[string]AggregateFn)}
	reg.RegisterAggregateFn(AggregateAVG, func(values []float64) float64 {
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	})
	return reg, nil
}

// RegisterAggregateFn registers an aggregate function under the given name for use in sampling policies.
func (ts *Registry) RegisterAggregateFn(name string, fn AggregateFn) {
	if ts.aggregateFns == nil {
		ts.aggregateFns = make(map[string]AggregateFn)
	}
	ts.aggregateFns[name] = fn
}

// SamplingPolicy defines the retention and precision for a series, and optionally an aggregate by name for collapsing buckets.
// Use AggregateAVG or a name registered via RegisterAggregateFn. Empty string means no aggregation (same as nil).
type SamplingPolicy struct {
	Retention   time.Duration
	Precision   time.Duration
	AggregateFn string // name of aggregate (e.g. AggregateAVG); empty = no aggregation
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

const mainPolicyName = "main"

// RegisterSeries inserts or updates a series.
// A main policy (Retention with positive retention duration) is required.
// Precision must be at least 1 second (sub-second precision is not supported).
func (ts *Registry) RegisterSeries(series TimeSeries) error {
	if series.Retention.Retention <= 0 || series.Retention.Precision <= 0 {
		return fmt.Errorf("main policy is required: retention and presicions must be set")
	}
	if series.Retention.Precision < time.Second {
		return fmt.Errorf("precision must be at least 1 second, got %v", series.Retention.Precision)
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
	aggr := data.AggregateFn
	existing, ok := existingMap[mainPolicyName]
	if ok {
		if existing.Precision == data.Precision && existing.Retention == data.Retention && existing.AggregationFn == aggr {
			return nil
		}
		existing.Precision = data.Precision
		existing.Retention = data.Retention
		existing.AggregationFn = aggr
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
		AggregationFn: aggr,
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
				out.Retention = SamplingPolicy{Precision: p.Precision, Retention: p.Retention, AggregateFn: p.AggregationFn}
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
			ret.Retention = SamplingPolicy{Precision: p.Precision, Retention: p.Retention, AggregateFn: p.AggregationFn}
			break
		}
	}
	return ret, err
}

// Maintenance runs background tasks (e.g. retention cleanup and bucket reduction) for all series.
// Errors from each series are collected and returned as a combined error; it does not stop on first failure.
func (ts *Registry) Maintenance(ctx context.Context) error {
	var dbSeries []dbTimeSeries
	if err := ts.db.Preload("Policies").Find(&dbSeries).Error; err != nil {
		return err
	}
	var errs []error
	for _, item := range dbSeries {
		if err := ts.cleanOneSeries(ctx, item); err != nil {
			errs = append(errs, fmt.Errorf("clean series %s: %w", item.Name, err))
		}
		if err := ts.reduceOneSeries(ctx, item.Name); err != nil {
			errs = append(errs, fmt.Errorf("reduce series %s: %w", item.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
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

// Implementation loads reduceBucketBatchSize buckets per query so memory is O(batchSize * records per bucket)
// and round-trips are reduced vs one query per bucket.
const reduceBucketBatchSize = 100

// reduceOneSeries iterates over the time buckets of the given series (using the main policy's precision).
// For each bucket that has more than one value, it replaces those values with a single value from the policy's aggregate (AggregateFn name).
// No-op if the policy's aggregate name is empty or not registered.
//
// Implementation uses batched bucket queries so memory is O(reduceBucketBatchSize * max records per bucket), not O(total records).
func (ts *Registry) reduceOneSeries(ctx context.Context, name string) error {
	mainPolicy, fn, err := ts.getSamplingPolicy(name, mainPolicyName)
	if err != nil {
		return err
	}
	if mainPolicy == nil || fn == nil {
		return nil
	}
	return ts.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&dbRecord{}).Where("sampling_id = ?", mainPolicy.ID).Count(&count).Error; err != nil {
			return fmt.Errorf("count: %w", err)
		}
		if count == 0 {
			return nil
		}
		var minRec, maxRec dbRecord
		if err := tx.Where("sampling_id = ?", mainPolicy.ID).Order("time ASC").First(&minRec).Error; err != nil {
			return fmt.Errorf("min time: %w", err)
		}
		if err := tx.Where("sampling_id = ?", mainPolicy.ID).Order("time DESC").First(&maxRec).Error; err != nil {
			return fmt.Errorf("max time: %w", err)
		}
		precision := mainPolicy.Precision
		minTime, maxTime := minRec.Time, maxRec.Time
		samplingID := mainPolicy.ID
		bucketStart := minTime.Truncate(precision)
		for !bucketStart.After(maxTime) {
			chunkEnd := bucketStart.Add(precision * reduceBucketBatchSize)
			if err := processReduceChunk(tx, samplingID, bucketStart, chunkEnd, precision, fn); err != nil {
				return err
			}
			bucketStart = chunkEnd
		}
		return nil
	})
}

func (ts *Registry) getSamplingPolicy(seriesName, PolicyName string) (*dbSamplingPolicy, AggregateFn, error) {
	if seriesName == "" {
		return nil, nil, fmt.Errorf("series name is required")
	}
	var s dbTimeSeries
	if err := ts.db.Preload("Policies").Where("name = ?", seriesName).First(&s).Error; err != nil {
		return nil, nil, fmt.Errorf("series not found: %w", err)
	}
	var mainPolicy *dbSamplingPolicy
	for i := range s.Policies {
		if s.Policies[i].Name == PolicyName {
			mainPolicy = &s.Policies[i]
			break
		}
	}
	if mainPolicy == nil {
		return nil, nil, fmt.Errorf("series has no main policy")
	}
	if mainPolicy.Precision <= 0 {
		return nil, nil, fmt.Errorf("main policy has invalid precision")
	}
	aggrName := mainPolicy.AggregationFn
	fn := ts.aggregateFns[aggrName]
	if fn == nil {
		return nil, nil, nil
	}
	return mainPolicy, fn, nil
}

type reduceBucket struct {
	records []dbRecord
}

func processReduceChunk(tx *gorm.DB, samplingID uint, chunkStart, chunkEnd time.Time, precision time.Duration, fn AggregateFn) error {
	var recs []dbRecord
	if err := tx.Where("sampling_id = ? AND time >= ? AND time < ?", samplingID, chunkStart, chunkEnd).Order("time ASC, id ASC").Find(&recs).Error; err != nil {
		return fmt.Errorf("list buckets: %w", err)
	}
	buckets := make(map[time.Time]*reduceBucket)
	for i := range recs {
		t := recs[i].Time.Truncate(precision)
		if buckets[t] == nil {
			buckets[t] = &reduceBucket{}
		}
		buckets[t].records = append(buckets[t].records, recs[i])
	}
	for bucketStart, b := range buckets {
		if err := reduceOneBucket(tx, bucketStart, b, fn); err != nil {
			return err
		}
	}
	return nil
}

func reduceOneBucket(tx *gorm.DB, bucketStart time.Time, b *reduceBucket, fn AggregateFn) error {
	keep := b.records[0]
	var value float64
	if len(b.records) > 1 {
		values := make([]float64, len(b.records))
		for i, r := range b.records {
			values[i] = r.Value
		}
		value = fn(values)
		for i := 1; i < len(b.records); i++ {
			if err := tx.Delete(&dbRecord{}, b.records[i].Id).Error; err != nil {
				return fmt.Errorf("delete duplicate in bucket: %w", err)
			}
		}
	} else {
		value = keep.Value
	}
	if err := tx.Model(&dbRecord{}).Where("id = ?", keep.Id).Updates(map[string]interface{}{"time": bucketStart, "value": value}).Error; err != nil {
		return fmt.Errorf("update record: %w", err)
	}
	return nil
}
