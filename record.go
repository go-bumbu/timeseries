package timeseries

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type dbRecord struct {
	Id         uint      `gorm:"primary_key"`
	SamplingId uint      `gorm:"index"`
	Time       time.Time `gorm:"index"`
	Value      float64
}

// Ingest adds a new data point and returns its ID.
func (ts *Registry) Ingest(series string, t time.Time, value float64) (uint, error) {
	if series == "" {
		return 0, fmt.Errorf("timeseries name cannot be empty")
	}
	if t.IsZero() {
		return 0, fmt.Errorf("timeseries time value cannot be zero")
	}

	var s dbTimeSeries
	if err := ts.db.Preload("Policies").Where("name = ?", series).First(&s).Error; err != nil {
		return 0, fmt.Errorf("failed to lookup series: %w", err)
	}

	item := dbRecord{
		SamplingId: s.mainPolicyID(),
		Time:       t,
		Value:      value,
	}
	if err := ts.db.Create(&item).Error; err != nil {
		return 0, err
	}
	return item.Id, nil
}

type RecordUpdate struct {
	Time  *time.Time
	Value *float64
}

// UpdateRecord updates an existing record
func (ts *Registry) UpdateRecord(Id uint, in RecordUpdate) error {
	if Id == 0 {
		return fmt.Errorf("record id is required for update")
	}

	var rec dbRecord
	if err := ts.db.First(&rec, Id).Error; err != nil {
		return fmt.Errorf("record not found: %w", err)
	}

	updates := make(map[string]interface{})
	if in.Time != nil {
		updates["time"] = *in.Time
	}
	if in.Value != nil {
		updates["value"] = *in.Value
	}

	return ts.db.Model(&rec).Updates(updates).Error
}

type Record struct {
	Id     uint
	Series string
	Time   time.Time
	Value  float64
}

// ListRecords returns all records for a given series (main retention policy).
// Optional start and end times can be provided to filter records by time range.
// If start is zero, no lower bound is applied. If end is zero, no upper bound is applied.
func (ts *Registry) ListRecords(name string, start, end time.Time) ([]Record, error) {
	if name == "" {
		return nil, fmt.Errorf("series name is required")
	}

	var s dbTimeSeries
	if err := ts.db.Preload("Policies").Where("name = ?", name).First(&s).Error; err != nil {
		return nil, fmt.Errorf("series not found: %w", err)
	}

	mainID := s.mainPolicyID()
	if mainID == 0 {
		return nil, fmt.Errorf("series has no main policy")
	}

	query := ts.db.Where("sampling_id = ?", mainID)

	// Apply time filters if provided
	if !start.IsZero() {
		query = query.Where("time >= ?", start)
	}
	if !end.IsZero() {
		query = query.Where("time <= ?", end)
	}

	var dbRecs []dbRecord
	if err := query.Order("time ASC").Find(&dbRecs).Error; err != nil {
		return nil, fmt.Errorf("failed to list records: %w", err)
	}

	out := make([]Record, len(dbRecs))
	for i, r := range dbRecs {
		out[i] = Record{
			Id:     r.Id,
			Series: name,
			Time:   r.Time,
			Value:  r.Value,
		}
	}

	return out, nil
}

// DeleteRecord removes a record by its id, does NOT return an error if the record does not exist
func (ts *Registry) DeleteRecord(id uint) error {
	if id == 0 {
		return fmt.Errorf("record id cannot be zero")
	}
	return ts.db.Delete(&dbRecord{}, id).Error
}

// RecordAt returns the latest record at or before the given time
func (ts *Registry) RecordAt(series string, t time.Time) (*Record, error) {
	if series == "" {
		return nil, fmt.Errorf("series name cannot be empty")
	}
	if t.IsZero() {
		return nil, fmt.Errorf("time cannot be zero")
	}

	var s dbTimeSeries
	if err := ts.db.Preload("Policies").Where("name = ?", series).First(&s).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("series not found")
		}
		return nil, fmt.Errorf("series not found: %w", err)
	}

	mainID := s.mainPolicyID()
	if mainID == 0 {
		return nil, fmt.Errorf("series has no main policy")
	}

	var r dbRecord
	err := ts.db.
		Where("sampling_id = ? AND time <= ?", mainID, t).
		Order("time desc").
		First(&r).Error
	if err != nil {
		return nil, err
	}

	return &Record{
		Id:     r.Id,
		Series: series,
		Time:   r.Time,
		Value:  r.Value,
	}, nil
}

// ValueAt returns the value of the latest record at or before the given time
func (ts *Registry) ValueAt(series string, t time.Time) (float64, error) {
	r, err := ts.RecordAt(series, t)
	if err != nil {
		return 0, err
	}
	return r.Value, nil
}
