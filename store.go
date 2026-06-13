package timeseries

import (
	"errors"
	"sync"

	"gorm.io/gorm"
)

// AggregateFn collapses the values in a precision bucket into one value.
// Values are passed in ascending time order (so first = values[0], last = values[len-1]).
type AggregateFn func(values []float64) float64

// ErrSeriesNotFound is returned (wrapped) when a named series does not exist.
// Test for it with errors.Is.
var ErrSeriesNotFound = errors.New("series not found")

// ErrFieldNotFound is returned (wrapped) when a field is not defined for a series.
// Test for it with errors.Is.
var ErrFieldNotFound = errors.New("field not found")

// Store is the time series handle.
//
// All exported methods serialize against each other through an internal
// RWMutex: structural operations (DefineSeries, DropSeries, Maintain,
// RegisterAggregate) take it exclusively, while reads and point writes take it
// shared. This makes a single Store safe for concurrent use and closes the
// orphan-record and reduce-window races between DefineSeries/Maintain and
// concurrent writers. It does NOT coordinate across multiple Store instances or
// other processes pointed at the same database — application-level integrity
// still assumes all writes go through one Store.
type Store struct {
	db         *gorm.DB
	mu         sync.RWMutex
	aggregates map[string]AggregateFn
}

// New migrates the schema and returns a Store with the built-in aggregates registered.
//
// New runs AutoMigrate on every call. Treat the returned Store as the single
// owner of its database: the serialization that protects against the
// orphan-record and reduce-window races is in-process only (see the Store doc
// comment), so a second Store — in this process or another — pointed at the same
// database can still cause silent data loss during Maintain. Run one Store per
// database.
func New(db *gorm.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("timeseries: db must not be nil")
	}
	// Dimension tables are ordinary rowid tables (they use autoincrement IDs,
	// which WITHOUT ROWID forbids).
	if err := db.AutoMigrate(&dbSeries{}, &dbField{}, &dbSeriesLabel{}); err != nil {
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

// RegisterAggregate registers (or overrides) an aggregate by name. It is safe
// to call concurrently with other Store operations.
func (s *Store) RegisterAggregate(name string, fn AggregateFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.register(name, fn)
}

// register stores an aggregate without locking. Callers must hold s.mu, or be
// constructing the Store before it is shared (as registerBuiltins does). The
// RWMutex is not reentrant, so never call this — or registerBuiltins — from a
// path that already holds the lock via the exported RegisterAggregate.
func (s *Store) register(name string, fn AggregateFn) {
	if s.aggregates == nil {
		s.aggregates = make(map[string]AggregateFn)
	}
	s.aggregates[name] = fn
}

func (s *Store) registerBuiltins() {
	s.register(AggAvg, aggAvg)
	s.register(AggSum, aggSum)
	s.register(AggMin, aggMin)
	s.register(AggMax, aggMax)
	s.register(AggFirst, aggFirst)
	s.register(AggLast, aggLast)
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
