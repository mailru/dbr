package dbr_test

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mailru/dbr"
	"github.com/mailru/dbr/dialect"
)

var rawData = getDataSlice(10000)

func Benchmark_SQLScan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRawSQL(b, rawData, []benchItem{})
	}
}

func Benchmark_SQLScanWithCap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRawSQL(b, rawData, make([]benchItem, 0, len(rawData)))
	}
}

func Benchmark_DBRLoad(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBR(b, rawData, []benchItem{})
	}
}

func Benchmark_DBRLoadPtrs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBRPtrs(b, rawData, []*benchItem{})
	}
}

func Benchmark_DBRLoadWithCap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBR(b, rawData, make([]benchItem, 0, len(rawData)))
	}
}

func Benchmark_DBRLoadPtrsWithCap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchDBR(b, rawData, make([]benchItem, 0, len(rawData)))
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

func getRowsMocked(b *testing.B, data []benchItem) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"field1", "field2"})
	for _, item := range data {
		rows.AddRow(item.Field1, item.Field2)
	}
	return rows
}

func benchRawSQL(b *testing.B, data []benchItem, res []benchItem) {
	b.StopTimer()
	db, mock, err := sqlmock.New()
	if err != nil {
		b.Error(err)
	}
	mock.ExpectQuery("select").WillReturnRows(getRowsMocked(b, data))
	b.StartTimer()

	rows, err := db.Query("select")
	if err != nil {
		b.Error(err)
	}

	var item benchItem
	for rows.Next() {
		if err := rows.Scan(&item.Field1, &item.Field2); err != nil {
			b.Error(err)
		}
		res = append(res, item)
	}
}

func benchDBR(b *testing.B, data []benchItem, res []benchItem) {
	b.StopTimer()
	sess, dbmock := getDBRMock(b, dialect.MySQL)
	dbmock.ExpectQuery("SELECT field1, field2 FROM sometable").WillReturnRows(getRowsMocked(b, data))
	rows := sess.Select("field1", "field2").From("sometable")
	b.StartTimer()

	if _, err := rows.LoadStructs(&res); err != nil {
		b.Error(err)
	}
}

func benchDBRPtrs(b *testing.B, data []benchItem, res []*benchItem) {
	b.StopTimer()
	sess, dbmock := getDBRMock(b, dialect.MySQL)
	dbmock.ExpectQuery("SELECT field1, field2 FROM sometable").WillReturnRows(getRowsMocked(b, data))
	rows := sess.Select("field1", "field2").From("sometable")
	b.StartTimer()

	if _, err := rows.LoadStructs(&res); err != nil {
		b.Error(err)
	}
}

func getDBRMock(b *testing.B, dialect dbr.Dialect) (*dbr.Session, sqlmock.Sqlmock) {
	db, dbmock, err := sqlmock.New()
	if err != nil {
		b.Error(err)
	}

	conn := dbr.Connection{DB: db, Dialect: dialect, EventReceiver: &dbr.NullEventReceiver{}}

	return conn.NewSession(conn.EventReceiver), dbmock
}
