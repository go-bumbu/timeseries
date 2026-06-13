package timeseries

import (
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestUnixMilli_RoundTrip(t *testing.T) {
	src := time.Date(2025, 1, 2, 15, 4, 5, 0, time.UTC)

	v, err := unixMilli(src).Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if v.(int64) != src.UnixMilli() {
		t.Fatalf("Value = %v, want %d", v, src.UnixMilli())
	}

	var got unixMilli
	if err := got.Scan(src.UnixMilli()); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !time.Time(got).Equal(src) {
		t.Fatalf("Scan round-trip = %v, want %v", time.Time(got), src)
	}
}

func TestUnixMilli_Zero(t *testing.T) {
	v, err := unixMilli(time.Time{}).Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if v != nil {
		t.Fatalf("zero time Value = %v, want nil", v)
	}
}

func TestUnixMilli_GormDataType(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	got := unixMilli{}.GormDBDataType(db, nil)
	if got != "INTEGER" {
		t.Fatalf("sqlite data type = %q, want INTEGER", got)
	}
}

func TestUnixMilli_ScanInvalidType(t *testing.T) {
	var got unixMilli
	if err := got.Scan("not a time"); err == nil {
		t.Fatal("Scan should reject unsupported type")
	}
}
