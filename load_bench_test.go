package dbr_test

// go test -benchmem -bench . load_bench_test.go

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mailru/dbr"
	"github.com/stretchr/testify/assert"
)

var (
	rawData = getDataSlice(10000)
)

func Benchmark_SQL_Scan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchSQLScan(b, rawData, getRowsRaw(b, rawData))
	}
}

func Benchmark_DBR_Load(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBRLoad(b, rawData, getRowsRaw(b, rawData))
	}
}

func Benchmark_DBR_LoadV2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBRLoadV2(b, rawData, getRowsRaw(b, rawData))
	}
}

type benchItem struct {
	Field1 string
	Field2 int
}

func getDataSlice(itemsCnt int) []benchItem {
	res := make([]benchItem, 0, itemsCnt)
	for num := 0; len(res) < cap(res); num++ {
		res = append(res, benchItem{Field1: "str" + fmt.Sprint(num), Field2: num})
	}
	return res
}

func getRowsMocked(data []benchItem) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"field1", "field2"})
	for _, item := range data {
		rows.AddRow(item.Field1, item.Field2)
	}
	return rows
}

func getRowsRaw(b *testing.B, data []benchItem) *sql.Rows {
	if b != nil {
		b.StopTimer()
		defer b.StartTimer()
	}

	db, mock, err := sqlmock.New()
	if b != nil {
		assert.NoError(b, err)
	}

	mock.ExpectQuery("select").WillReturnRows(getRowsMocked(data))

	res, err := db.Query("select")
	if b != nil {
		assert.NoError(b, err)
	}

	return res
}

func benchSQLScan(b *testing.B, expected []benchItem, iter *sql.Rows) {
	defer iter.Close()

	res := make([]benchItem, 0, len(expected))
	var item benchItem

	for iter.Next() {
		if err := iter.Scan(&item.Field1, &item.Field2); err != nil {
			panic(err)
		}
		res = append(res, item)
	}

	b.StopTimer()
	assert.EqualValues(b, expected, res)
	b.StartTimer()
}

func benchDBRLoad(b *testing.B, expected []benchItem, iter *sql.Rows) {
	defer iter.Close()

	res := make([]benchItem, 0, len(expected))
	if _, err := dbr.Load(iter, &res); err != nil {
		panic(err)
	}

	b.StopTimer()
	assert.EqualValues(b, expected, res)
	b.StartTimer()
}

func benchDBRLoadV2(b *testing.B, expected []benchItem, iter *sql.Rows) {
	defer iter.Close()

	res := make([]benchItem, 0, len(expected))
	if _, err := dbr.LoadV2(iter, &res); err != nil {
		panic(err)
	}

	b.StopTimer()
	assert.EqualValues(b, expected, res)
	b.StartTimer()
}
