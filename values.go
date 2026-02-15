package timeseries

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// RecordAt returns the latest record at or before the given time
func (ts *Registry) RecordAt(series string, t time.Time) (*Record, error) {
	if series == "" {
		return nil, fmt.Errorf("series name cannot be empty")
	}
	if t.IsZero() {
		return nil, fmt.Errorf("time cannot be zero")
	}

	// Find the series
	var s dbTimeSeries
	if err := ts.db.Preload("Policies").Where("name = ?", series).First(&s).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("series not found")
		}
		return nil, fmt.Errorf("series not found: %w", err)
	}

	// Collect all policy IDs for this series
	policyIDs := make([]uint, len(s.Policies))
	for i, policy := range s.Policies {
		policyIDs[i] = policy.ID
	}

	// Find the latest record at or before t from any policy
	var r dbRecord
	err := ts.db.
		Where("sampling_id IN ? AND time <= ?", policyIDs, t).
		Order("time desc").
		First(&r).Error
	if err != nil {
		return nil, err
	}

	// Map dbRecord to Record
	return &Record{
		Id:     r.Id,
		Series: series,
		Time:   r.Time,
		Value:  r.Value,
	}, nil
}

// ValueAt is a wrapper function that only returns the value of a time series at a specific time
func (ts *Registry) ValueAt(series string, t time.Time) (float64, error) {
	r, err := ts.RecordAt(series, t)
	if err != nil {
		return 0, err
	}
	return r.Value, nil
}
