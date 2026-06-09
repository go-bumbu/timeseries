package timeseries

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// reduceChunkBuckets controls how many precision buckets are loaded per query.
const reduceChunkBuckets = 100

// Maintain runs retention cleanup and per-field bucket reduction for all series.
// Errors are collected per series; it does not stop on the first failure.
func (s *Store) Maintain(ctx context.Context) error {
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
	cutoff := time.Now().Add(-ser.Retention)
	return s.db.WithContext(ctx).
		Where("series_id = ? AND time < ?", ser.ID, unixMilli(cutoff)).
		Delete(&dbRecord{}).Error
}

// reduceSeries collapses multi-record precision buckets per field using the
// field's aggregate function.
func (s *Store) reduceSeries(ctx context.Context, ser dbSeries) error {
	var fields []dbField
	if err := s.db.WithContext(ctx).Find(&fields).Error; err != nil {
		return err
	}
	for _, f := range fields {
		if f.AggregateFn == "" {
			continue
		}
		fn, ok := s.aggregates[f.AggregateFn]
		if !ok {
			continue // unknown aggregate: skip (validated at DefineField, defensive here)
		}
		if err := s.reduceField(ctx, ser, f, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) reduceField(ctx context.Context, ser dbSeries, f dbField, fn AggregateFn) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var minRec, maxRec dbRecord
		if err := tx.Where("series_id = ? AND field_id = ?", ser.ID, f.ID).
			Order("time ASC").Limit(1).Find(&minRec).Error; err != nil {
			return err
		}
		if err := tx.Where("series_id = ? AND field_id = ?", ser.ID, f.ID).
			Order("time DESC").Limit(1).Find(&maxRec).Error; err != nil {
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
			if err := s.reduceChunk(tx, ser.ID, f.ID, bucket, chunkEnd, precision, fn); err != nil {
				return err
			}
			bucket = chunkEnd
		}
		return nil
	})
}

func (s *Store) reduceChunk(tx *gorm.DB, seriesID, fieldID uint, start, end time.Time, precision time.Duration, fn AggregateFn) error {
	var recs []dbRecord
	if err := tx.Where("series_id = ? AND field_id = ? AND time >= ? AND time < ?",
		seriesID, fieldID, unixMilli(start), unixMilli(end)).
		Order("time ASC").Find(&recs).Error; err != nil {
		return err
	}

	type group struct {
		recs []dbRecord
	}
	buckets := map[time.Time]*group{}
	var order []time.Time
	for _, r := range recs {
		b := r.Time.asTime().Truncate(precision)
		if buckets[b] == nil {
			buckets[b] = &group{}
			order = append(order, b)
		}
		buckets[b].recs = append(buckets[b].recs, r)
	}

	for _, b := range order {
		g := buckets[b]
		if len(g.recs) < 2 {
			continue // already one row in the bucket
		}
		vals := make([]float64, len(g.recs))
		for i, r := range g.recs {
			vals[i] = r.Value // recs are time-ascending: the aggregate contract holds
		}
		reduced := fn(vals)

		// Delete every raw row in the bucket, then write one row at the bucket start.
		if err := tx.Where("series_id = ? AND field_id = ? AND time >= ? AND time < ?",
			seriesID, fieldID, unixMilli(b), unixMilli(b.Add(precision))).
			Delete(&dbRecord{}).Error; err != nil {
			return err
		}
		if err := tx.Create(&dbRecord{
			SeriesId: seriesID,
			FieldId:  fieldID,
			Time:     unixMilli(b),
			Value:    reduced,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}
