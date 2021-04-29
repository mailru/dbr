package dbr

import (
	"testing"

	"github.com/mailru/dbr/dialect"
	"github.com/stretchr/testify/assert"
)

func TestSelectStmt(t *testing.T) {
	bufClickHouse := NewBuffer()
	bufMySQL := NewBuffer()
	builder := Select("a", "b").
		From(Select("a").From("table")).
		LeftJoin("table2", "table.a1 = table.a2").
		Distinct().
		Prewhere(Eq("c1", 15)).
		Where(Eq("c2", 1)).
		GroupBy("d").
		Having(Eq("e", 2)).
		OrderAsc("f").
		Limit(3).
		Offset(4).
		ForUpdate().
		SkipLocked()

	err := builder.Build(dialect.ClickHouse, bufClickHouse) // because this lib is clickhouse first.
	assert.NoError(t, err)
	assert.Equal(t, "SELECT DISTINCT a, b FROM ? LEFT JOIN `table2` ON table.a1 = table.a2 PREWHERE (`c1` = ?) WHERE (`c2` = ?) GROUP BY d HAVING (`e` = ?) ORDER BY f ASC LIMIT 4,3 FOR UPDATE SKIP LOCKED", bufClickHouse.String())
	assert.Equal(t, 4, len(bufClickHouse.Value()))

	err = builder.Build(dialect.MySQL, bufMySQL)
	assert.EqualError(t, err, ErrPrewhereNotSupported.Error()) // handle PREWHERE statement error
}

func BenchmarkSelectSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		Select("a", "b").From("table").Where(Eq("c", 1)).OrderAsc("d").Build(dialect.MySQL, buf)
	}
}
