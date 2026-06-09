package timeseries

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbField is the global field dimension (the external key).
type dbField struct {
	ID          uint   `gorm:"primaryKey;autoIncrement"`
	Name        string `gorm:"uniqueIndex;not null;size:64"`
	AggregateFn string `gorm:"not null;size:32"`
}

func (dbField) TableName() string { return "fields" }

// Field is a globally-defined measurement name with its bucket aggregation.
type Field struct {
	Name      string
	Aggregate string // AggLast, AggMax, ...; "" means no bucket reduction
}

// DefineField creates or updates a field by name. The aggregate name must be
// empty or previously registered; otherwise it errors.
func (s *Store) DefineField(f Field) error {
	if f.Name == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	if f.Aggregate != "" {
		if _, ok := s.aggregates[f.Aggregate]; !ok {
			return fmt.Errorf("unknown aggregate %q", f.Aggregate)
		}
	}
	row := dbField{Name: f.Name, AggregateFn: f.Aggregate}
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"aggregate_fn"}),
	}).Create(&row).Error
}

// ListFields returns all defined fields.
func (s *Store) ListFields() ([]Field, error) {
	var rows []dbField
	if err := s.db.Order("name ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]Field, len(rows))
	for i, r := range rows {
		out[i] = Field{Name: r.Name, Aggregate: r.AggregateFn}
	}
	return out, nil
}

// fieldID resolves a field name to its id; errors if undefined.
func (s *Store) fieldID(name string) (uint, error) {
	var f dbField
	if err := s.db.Where("name = ?", name).First(&f).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, fmt.Errorf("field %q is not defined", name)
		}
		return 0, err
	}
	return f.ID, nil
}
