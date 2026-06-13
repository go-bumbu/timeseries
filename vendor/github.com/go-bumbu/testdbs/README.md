# testdbs
Utility package to start and stop different DBs in tests

# Usage

## Modify your tests

Modify the Main test function to initialize the DBs before the tests are run and to delete after

```
func TestMain(m *testing.M) {
	testdbs.InitDBS()
	// main block that runs tests
	code := m.Run()
	err := testdbs.Clean()
	if err != nil {
		os.Exit(1)
	}
	os.Exit(code)
}
```

And then in your tests you can iterate over the DBs

```
func TestMyFunction(t *testing.T) {
    for _, db := range testdbs.DBs() {
        t.Run(dbt.DbType(), func(t *testing.T) {
            db := dbt.Conn()
            // db is *gorm.DB
		})
	}
}
```

if you need to isolate DBs, e.g. run multiple tests on the same db/table
you can use  `ConnDbName("custom")` to crete a new database wit the passed name.

note: connections will be reused cross tests for every db name

```
func TestMyFunction(t *testing.T) {
    for _, db := range testdbs.DBs() {
        t.Run(dbt.DbType(), func(t *testing.T) {
            db := dbt.ConnDbName("custom")
            // db is *gorm.DB
		})
	}
}
```


## running tests

As a default calling `go test` will only start an embedded sqlite on a temp directory, to run the tests with
all the supported DBs you need to call `go test -alldbs`

## sqlite

if you want to inspect the sqlite database after running the tests you can set the env `LOCAL_SQLITE` to true
and this will create the DB in the current working dir

```
export LOCAL_SQLITE=true
```