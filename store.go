package timeseries

import "gorm.io/gorm"

// AggregateFn collapses the values in a precision bucket into one value.
// Values are passed in ascending time order (so first = values[0], last = values[len-1]).
type AggregateFn func(values []float64) float64

// Store is the time series handle.
type Store struct {
	db         *gorm.DB
	aggregates map[string]AggregateFn
}

// New migrates the schema and returns a Store with the built-in aggregates registered.
func New(db *gorm.DB) (*Store, error) {
	// Dimension tables are ordinary rowid tables (they use autoincrement IDs,
	// which WITHOUT ROWID forbids).
	if err := db.AutoMigrate(&dbSeries{}, &dbField{}); err != nil {
		return nil, err
	}

	// The records table is clustered on its composite PK on SQLite.
	// table_options is appended after the column list, exactly where SQLite wants it.
	rec := db
	if db.Name() == "sqlite" {
		rec = db.Set("gorm:table_options", "WITHOUT ROWID")
	}
	if err := rec.AutoMigrate(&dbRecord{}); err != nil {
		return nil, err
	}

	s := &Store{db: db, aggregates: make(map[string]AggregateFn)}
	s.registerBuiltins()
	return s, nil
}

const (
	AggAvg   = "avg"
	AggSum   = "sum"
	AggMin   = "min"
	AggMax   = "max"
	AggFirst = "first"
	AggLast  = "last"
)

// RegisterAggregate registers (or overrides) an aggregate by name.
func (s *Store) RegisterAggregate(name string, fn AggregateFn) {
	if s.aggregates == nil {
		s.aggregates = make(map[string]AggregateFn)
	}
	s.aggregates[name] = fn
}

func (s *Store) registerBuiltins() {
	s.RegisterAggregate(AggAvg, aggAvg)
	s.RegisterAggregate(AggSum, aggSum)
	s.RegisterAggregate(AggMin, aggMin)
	s.RegisterAggregate(AggMax, aggMax)
	s.RegisterAggregate(AggFirst, aggFirst)
	s.RegisterAggregate(AggLast, aggLast)
}

// All built-ins are only ever called with a non-empty slice (the reducer guarantees it).
func aggAvg(v []float64) float64 {
	var sum float64
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}

func aggSum(v []float64) float64 {
	var sum float64
	for _, x := range v {
		sum += x
	}
	return sum
}

func aggMin(v []float64) float64 {
	m := v[0]
	for _, x := range v[1:] {
		if x < m {
			m = x
		}
	}
	return m
}

func aggMax(v []float64) float64 {
	m := v[0]
	for _, x := range v[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

func aggFirst(v []float64) float64 { return v[0] }
func aggLast(v []float64) float64  { return v[len(v)-1] }
