package timeseries

import (
	"fmt"
	"time"
)

type Record struct {
	Id     uint
	Series string
	Time   time.Time
	Value  float64
}

type dbRecord struct {
	Id         uint      `gorm:"primary_key"`
	SamplingId uint      `gorm:"index"`
	Time       time.Time `gorm:"index"`
	Value      float64
}

// Ingest adds a new data point
func (ts *Registry) Ingest(in Record) error {
	if in.Series == "" {
		return fmt.Errorf("timeseries name cannot be empty")
	}
	if in.Time.IsZero() {
		return fmt.Errorf("timeseries time value cannot be zero")
	}

	// Ensure series exists (create if missing)
	var s dbTimeSeries
	err := ts.db.Preload("Policies").Where("name = ?", in.Series).First(&s).Error
	if err != nil {
		return fmt.Errorf("failed to lookup series: %w", err)
	}

	item := dbRecord{
		SamplingId: s.mainPolicyID(),
		Time:       in.Time,
		Value:      in.Value,
	}
	return ts.db.Create(&item).Error
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

// ListRecords returns all records for a given series name from all sampling policies.
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

	// Collect all policy IDs for this series
	policyIDs := make([]uint, len(s.Policies))
	for i, policy := range s.Policies {
		policyIDs[i] = policy.ID
	}

	// Query records from all policies
	query := ts.db.Where("sampling_id IN ?", policyIDs)

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
