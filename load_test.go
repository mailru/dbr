package dbr

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mailru/dbr/dialect"
	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	type testStruct struct {
		A string
	}

	testcases := []struct {
		columns  []string
		expected interface{}
	}{
		{[]string{"a"}, "a"},
		{[]string{"a"}, []string{"a"}},
		{[]string{"a"}, testStruct{"a"}},
		{[]string{"a"}, &testStruct{"a"}},
		{[]string{"a"}, []testStruct{{"a"}}},
		{[]string{"a"}, []*testStruct{{"a"}}},
		{[]string{"a", "b"}, map[string]interface{}{"a": "a", "b": "b"}},
		{[]string{"a", "b"}, &map[string]interface{}{"a": "a", "b": "b"}},
		{[]string{"a", "b"}, []map[string]interface{}{{"a": "a", "b": "b"}}},
	}

	for _, tc := range testcases {
		var values []driver.Value
		session, dbmock := newSessionMock()
		for _, c := range tc.columns {
			values = append(values, c)
		}
		rows := sqlmock.NewRows(tc.columns).AddRow(values...)
		dbmock.ExpectQuery("SELECT .+").WillReturnRows(rows)
		v := reflect.New(reflect.TypeOf(tc.expected)).Elem().Addr().Interface()
		session.Select(tc.columns...).From("table").Load(v)
		assert.Equal(t, tc.expected, reflect.Indirect(reflect.ValueOf(v)).Interface())
	}
}

func TestLoadWithBytesValue(t *testing.T) {
	var values []driver.Value
	columns := []string{"fieldname"}
	value := []byte("fieldvalue")
	session, dbmock := newSessionMock()
	values = append(values, value)
	rows := sqlmock.NewRows(columns).AddRow(values...)
	dbmock.ExpectQuery("SELECT .+").WillReturnRows(rows)
	v := reflect.New(reflect.TypeOf(map[string]interface{}(nil))).Elem().Addr().Interface()
	session.Select(columns...).From("table").Load(v)
	value[0] = byte('a')
	assert.Equal(t, map[string]interface{}{"fieldname": []byte("fieldvalue")},
		reflect.Indirect(reflect.ValueOf(v)).Interface())
}

func BenchmarkLoad(b *testing.B) {
	session, dbmock := newSessionMock()
	rows := sqlmock.NewRows([]string{"a", "b", "c"})
	for i := 0; i < 100; i++ {
		rows = rows.AddRow(1, 2, 3)
	}
	dbmock.ExpectQuery("SELECT a, b, c FROM table").WillReturnRows(rows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := make([]struct {
			A int `db:"a"`
			B int `db:"b"`
			C int
			D int `db:"-"`
			e int
			F int `db:"f"`
			G int `db:"g"`
			H int `db:"h"`
			i int
			j int
		}, 0, 100)
		session.Select("a", "b", "c").From("table").LoadStructs(&r)
	}
}

func newSessionMock() (SessionRunner, sqlmock.Sqlmock) {
	db, m, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	conn := Connection{DBConn: db, Dialect: dialect.MySQL, EventReceiver: nullReceiver}
	return conn.NewSession(nil), m
}

func Test_Load_Scalar(t *testing.T) {
	t.Parallel()
	var res int
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"cnt"}).AddRow(123)), &res)
	assert.NoError(t, err)
	assert.EqualValues(t, 123, res)
}

func Test_Load_ScalarPtr(t *testing.T) {
	t.Parallel()
	var res *int
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"cnt"}).AddRow(123)), &res)
	assert.NoError(t, err)
	expected := new(int)
	*expected = 123
	assert.EqualValues(t, expected, res)
}

func Test_Load_ScalarSlice(t *testing.T) {
	t.Parallel()
	var res []int
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"cnt"}).AddRow(111).AddRow(222).AddRow(333)), &res)
	assert.NoError(t, err)
	assert.EqualValues(t, []int{111, 222, 333}, res)
}

func Test_Load_ScalarSlicePtr(t *testing.T) {
	t.Parallel()
	var expected, actual []*int
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"cnt"}).AddRow(0).AddRow(1).AddRow(2)), &actual)
	assert.NoError(t, err)
	for k := range make([]int, 3) {
		k := k
		expected = append(expected, &k)
	}
	assert.EqualValues(t, expected, actual)
}

type testObj struct {
	Field1 string
	Field2 int
}

func Test_Load_Struct(t *testing.T) {
	t.Parallel()
	var res testObj
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"field1", "field2"}).AddRow("111", 222)), &res)
	assert.NoError(t, err)
	assert.EqualValues(t, testObj{"111", 222}, res)
}

func Test_Load_StructPtr(t *testing.T) {
	t.Parallel()
	res := &testObj{}
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"field1", "field2"}).AddRow("111", 222)), &res)
	assert.NoError(t, err)
	assert.EqualValues(t, &testObj{"111", 222}, res)
}

func Test_Load_StructSlice(t *testing.T) {
	t.Parallel()
	var res []testObj
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"field1", "field2"}).AddRow("111", 222).AddRow("222", 333)), &res)
	assert.NoError(t, err)
	assert.EqualValues(t, []testObj{{"111", 222}, {"222", 333}}, res)
}

func Test_Load_StructSlicePtr(t *testing.T) {
	t.Parallel()
	var expected, actual []*testObj
	_, err := Load(sqlRows(t, sqlmock.NewRows([]string{"field1", "field2"}).AddRow("0", 0).AddRow("1", 1)), &actual)
	assert.NoError(t, err)
	for k := range make([]int, 2) {
		k := k
		expected = append(expected, &testObj{fmt.Sprint(k), k})
	}
	assert.EqualValues(t, expected, actual)
}

func sqlRows(t *testing.T, mockedRows *sqlmock.Rows) *sql.Rows {
	t.Helper()

	db, dbmock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}

	dbmock.ExpectQuery("select").WillReturnRows(mockedRows)

	rows, err := db.Query("select")
	if err != nil {
		t.Error(err)
	}

	return rows
}
