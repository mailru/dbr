package dbr

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type innerTestStruct struct {
	InnerTime    time.Time
	InnerNonTime int64
}

type testStruct struct {
	innerTestStruct
	Time          time.Time
	Inner         innerTestStruct
	InnerPtr      *innerTestStruct
	InnerSlice    []innerTestStruct
	InnerSlicePtr []*innerTestStruct
	InnerMap      map[int]*innerTestStruct
}

func TestChangeTimezone(t *testing.T) {
	location := "America/New_York"

	v := testStruct{
		innerTestStruct: innerTestStruct{
			InnerTime: time.Date(2020, 1, 20, 8, 0, 0, 0, time.UTC),
		},
		Time: time.Date(2020, 1, 21, 9, 0, 0, 0, time.UTC),
		Inner: innerTestStruct{
			InnerTime: time.Date(2020, 1, 22, 10, 0, 0, 0, time.UTC),
		},
		InnerPtr: &innerTestStruct{
			InnerTime: time.Date(2020, 1, 23, 11, 0, 0, 0, time.UTC),
		},
		InnerSlice: []innerTestStruct{
			{InnerTime: time.Date(2020, 1, 24, 12, 0, 0, 0, time.UTC)},
			{InnerTime: time.Date(2020, 1, 25, 13, 0, 0, 0, time.UTC)},
		},
		InnerSlicePtr: []*innerTestStruct{
			{InnerTime: time.Date(2020, 1, 26, 14, 0, 0, 0, time.UTC)},
			{InnerTime: time.Date(2020, 1, 27, 15, 0, 0, 0, time.UTC)},
		},
		InnerMap: map[int]*innerTestStruct{
			1: {InnerTime: time.Date(2020, 1, 28, 16, 0, 0, 0, time.UTC)},
			2: {InnerTime: time.Date(2020, 1, 28, 16, 0, 0, 0, time.UTC)},
		},
	}

	b := &selectBuilder{}
	l, _ := time.LoadLocation(location)
	b.InTimezone(l)
	b.changeTimezone(reflect.ValueOf(&v))

	assert.Equal(t, "America/New_York", v.InnerTime.Location().String())
	assert.Equal(t, "America/New_York", v.Time.Location().String())
	assert.Equal(t, "America/New_York", v.Inner.InnerTime.Location().String())
	assert.Equal(t, "America/New_York", v.InnerPtr.InnerTime.Location().String())
	for _, tt := range v.InnerSlice {
		assert.Equal(t, "America/New_York", tt.InnerTime.Location().String())
	}

	for _, tt := range v.InnerSlicePtr {
		assert.Equal(t, "America/New_York", tt.InnerTime.Location().String())
	}

	for _, tt := range v.InnerMap {
		assert.Equal(t, "America/New_York", tt.InnerTime.Location().String())
	}
}
