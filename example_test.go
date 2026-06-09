package timeseries_test

import (
	"fmt"
	"time"

	"github.com/go-bumbu/timeseries"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func ExampleStore() {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	ts, err := timeseries.New(db)
	if err != nil {
		fmt.Println(err)
		return
	}

	_ = ts.DefineSeries(timeseries.Series{
		Name:      "AAPL",
		Precision: 24 * time.Hour,
		Retention: 10 * 365 * 24 * time.Hour,
		Fields: []timeseries.Field{
			{Name: "close", Aggregate: timeseries.AggLast},
			{Name: "high", Aggregate: timeseries.AggMax},
		},
	})

	day := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	_ = ts.Write("AAPL", timeseries.Point{Time: day, Values: map[string]float64{"close": 102.1, "high": 103.0}})

	v, _, _ := ts.FieldAt("AAPL", "close", day)
	fmt.Printf("close=%.1f\n", v)

	// Output:
	// close=102.1
}
