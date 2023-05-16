package dbr

import (
	"database/sql"
	"reflect"
)

type loadType string

const (
	LoaderDefault loadType = ""
	LoaderV2      loadType = "loaderV2"
)

var LoadMode = LoaderDefault

// LoadV2 loads any value from sql.Rows.
func LoadV2(rows *sql.Rows, value interface{}) (int, error) {
	defer rows.Close()

	column, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, ErrInvalidPointer
	}

	v = v.Elem()
	isSlice := v.Kind() == reflect.Slice && v.Type().Elem().Kind() != reflect.Uint8
	count := 0

	var elemType reflect.Type
	if isSlice {
		elemType = v.Type().Elem()
	} else {
		elemType = v.Type()
	}

	var elem reflect.Value
	if isSlice {
		elem = reflect.New(v.Type().Elem()).Elem()
	} else {
		elem = v
	}

	extractor, err := findExtractor(elemType)
	if err != nil {
		return 0, err
	}

	ptr := extractor(column, elem)

	for rows.Next() {
		err = rows.Scan(ptr...)
		if err != nil {
			return count, err
		}

		count++

		if isSlice {
			v.Set(reflect.Append(v, elem))
		} else {
			break
		}
	}

	return count, rows.Err()
}
