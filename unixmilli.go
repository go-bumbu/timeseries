package timeseries

import (
	"database/sql/driver"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// unixMilli persists a time.Time as INTEGER unix milliseconds (UTC).
// The public API uses time.Time; this type is internal to the storage layer.
type unixMilli time.Time

// Value implements driver.Valuer: stores epoch milliseconds, or NULL for the zero time.
func (t unixMilli) Value() (driver.Value, error) {
	tt := time.Time(t)
	if tt.IsZero() {
		return nil, nil
	}
	return tt.UTC().UnixMilli(), nil
}

// Scan implements sql.Scanner: accepts int64 epoch ms (or time.Time from some drivers).
func (t *unixMilli) Scan(v any) error {
	switch n := v.(type) {
	case nil:
		*t = unixMilli(time.Time{})
	case int64:
		*t = unixMilli(time.UnixMilli(n).UTC())
	case time.Time:
		*t = unixMilli(n.UTC())
	default:
		return fmt.Errorf("unixMilli: cannot scan %T", v)
	}
	return nil
}

// GormDBDataType maps to an integer column per dialect.
func (unixMilli) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db.Name() == "sqlite" {
		return "INTEGER"
	}
	return "BIGINT"
}

func (t unixMilli) asTime() time.Time { return time.Time(t) }
