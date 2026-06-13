package timeseries

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// reduceChunkBuckets controls how many precision buckets are loaded per query.
const reduceChunkBuckets = 100

// nowFunc returns the current time used to compute the retention cutoff. It is a
// package-level seam so tests can pin the cutoff deterministically; production
// code never overrides it.
var nowFunc = time.Now

// Maintain runs retention cleanup and per-field bucket reduction for all series.
// Errors are collected per series; it does not stop on the first failure.
//
// Maintain takes the Store lock exclusively, so it cannot run concurrently with
// writes or reads on the same Store. This closes the window where a point
// written into a bucket between the reducer's read and its delete-then-recreate
// would be lost. Because it holds the lock for the whole sweep, run it from a
// dedicated maintenance goroutine, not on a hot read/write path.
func (s *Store) Maintain(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []dbSeries
	if err := s.db.WithContext(ctx).Find(&all).Error; err != nil {
		return err
	}
	var errs []error
	for _, ser := range all {
		if err := s.cleanRetention(ctx, ser); err != nil {
			errs = append(errs, err)
		}
		if err := s.reduceSeries(ctx, ser); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// cleanRetention deletes records older than the series retention.
func (s *Store) cleanRetention(ctx context.Context, ser dbSeries) error {
	cutoff := nowFunc().Add(-ser.Retention)
	return s.db.WithContext(ctx).
		Where("series_id = ? AND time < ?", ser.ID, unixMilli(cutoff)).
		Delete(&dbRecord{}).Error
}

// reduceSeries collapses multi-record precision buckets for a whole series in a
// single time-ordered pass, applying each field's aggregate. Fields with an
// empty aggregate are left untouched.
func (s *Store) reduceSeries(ctx context.Context, ser dbSeries) error {
	var fields []dbField
	if err := s.db.WithContext(ctx).Where("series_id = ?", ser.ID).Find(&fields).Error; err != nil {
		return err
	}
	aggByField := map[uint]AggregateFn{}
	for _, f := range fields {
		if f.AggregateFn == "" {
			continue
		}
		if fn, ok := s.aggregates[f.AggregateFn]; ok {
			aggByField[f.ID] = fn
		}
	}
	if len(aggByField) == 0 {
		return nil // nothing reducible
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Overall time span for the series.
		var minRec, maxRec dbRecord
		if err := tx.Where("series_id = ?", ser.ID).Order("time ASC").Limit(1).Find(&minRec).Error; err != nil {
			return err
		}
		if err := tx.Where("series_id = ?", ser.ID).Order("time DESC").Limit(1).Find(&maxRec).Error; err != nil {
			return err
		}
		if minRec.Time.asTime().IsZero() {
			return nil // no rows
		}

		precision := ser.Precision
		bucket := minRec.Time.asTime().Truncate(precision)
		maxTime := maxRec.Time.asTime()
		for !bucket.After(maxTime) {
			chunkEnd := bucket.Add(precision * reduceChunkBuckets)
			if err := s.reduceSeriesChunk(tx, ser.ID, bucket, chunkEnd, precision, aggByField); err != nil {
				return err
			}
			bucket = chunkEnd
		}
		return nil
	})
}

// reduceSeriesChunk reduces one bucket-aligned chunk [start, end) for all
// reducible fields of a series. Buckets never span chunk boundaries because the
// chunk size is a whole multiple of precision.
func (s *Store) reduceSeriesChunk(tx *gorm.DB, seriesID uint, start, end time.Time, precision time.Duration, aggByField map[uint]AggregateFn) error {
	var recs []dbRecord
	if err := tx.Where("series_id = ? AND time >= ? AND time < ?",
		seriesID, unixMilli(start), unixMilli(end)).
		Order("time ASC").Find(&recs).Error; err != nil {
		return err
	}

	type key struct {
		field  uint
		bucket time.Time
	}
	groups := map[key][]float64{}
	var order []key
	for _, r := range recs {
		if _, ok := aggByField[r.FieldID]; !ok {
			continue // field has no aggregate: leave its rows untouched
		}
		k := key{field: r.FieldID, bucket: r.Time.asTime().Truncate(precision)}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r.Value) // recs are time-ascending: aggregate contract holds
	}

	for _, k := range order {
		vals := groups[k]
		if len(vals) < 2 {
			continue // already one row in the bucket
		}
		reduced := aggByField[k.field](vals)
		if err := tx.Where("series_id = ? AND field_id = ? AND time >= ? AND time < ?",
			seriesID, k.field, unixMilli(k.bucket), unixMilli(k.bucket.Add(precision))).
			Delete(&dbRecord{}).Error; err != nil {
			return err
		}
		if err := tx.Create(&dbRecord{
			SeriesID: seriesID,
			FieldID:  k.field,
			Time:     unixMilli(k.bucket),
			Value:    reduced,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}
