package dbr

import (
	"context"
	"database/sql"
	"reflect"
	"time"
)

// SelectBuilder build "SELECT" stmt
type SelectBuilder interface {
	Builder
	EventReceiver
	loader
	typesLoader

	As(alias string) Builder
	Comment(text string) SelectBuilder
	Distinct() SelectBuilder
	ForUpdate() SelectBuilder
	From(table interface{}) SelectBuilder
	FullJoin(table, on interface{}) SelectBuilder
	GroupBy(col ...string) SelectBuilder
	Having(query interface{}, value ...interface{}) SelectBuilder
	InTimezone(loc *time.Location) SelectBuilder
	Join(table, on interface{}) SelectBuilder
	LeftJoin(table, on interface{}) SelectBuilder
	Limit(n uint64) SelectBuilder
	Offset(n uint64) SelectBuilder
	OrderAsc(col string) SelectBuilder
	OrderBy(col string) SelectBuilder
	OrderDesc(col string) SelectBuilder
	OrderDir(col string, isAsc bool) SelectBuilder
	Paginate(page, perPage uint64) SelectBuilder
	Prewhere(query interface{}, value ...interface{}) SelectBuilder
	RightJoin(table, on interface{}) SelectBuilder
	SkipLocked() SelectBuilder
	Where(query interface{}, value ...interface{}) SelectBuilder
}

type selectBuilder struct {
	runner
	EventReceiver

	Dialect    Dialect
	selectStmt *selectStmt
	timezone   *time.Location
}

func prepareSelect(a []string) []interface{} {
	b := make([]interface{}, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

// Select creates a SelectBuilder
func (sess *Session) Select(column ...string) SelectBuilder {
	return &selectBuilder{
		runner:        sess,
		EventReceiver: sess.EventReceiver,
		Dialect:       sess.Dialect,
		selectStmt:    createSelectStmt(prepareSelect(column)),
	}
}

// Select creates a SelectBuilder
func (tx *Tx) Select(column ...string) SelectBuilder {
	return &selectBuilder{
		runner:        tx,
		EventReceiver: tx.EventReceiver,
		Dialect:       tx.Dialect,
		selectStmt:    createSelectStmt(prepareSelect(column)),
	}
}

// SelectBySql creates a SelectBuilder from raw query
func (sess *Session) SelectBySql(query string, value ...interface{}) SelectBuilder {
	return &selectBuilder{
		runner:        sess,
		EventReceiver: sess.EventReceiver,
		Dialect:       sess.Dialect,
		selectStmt:    createSelectStmtBySQL(query, value),
	}
}

// SelectBySql creates a SelectBuilder from raw query
func (tx *Tx) SelectBySql(query string, value ...interface{}) SelectBuilder {
	return &selectBuilder{
		runner:        tx,
		EventReceiver: tx.EventReceiver,
		Dialect:       tx.Dialect,
		selectStmt:    createSelectStmtBySQL(query, value),
	}
}

func (b *selectBuilder) changeTimezone(value reflect.Value) {
	v, t := extractOriginal(value)
	switch t {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			b.changeTimezone(v.Index(i))
		}
	case reflect.Map:
		// TODO: add timezone changing for map keys
		for _, k := range v.MapKeys() {
			b.changeTimezone(v.MapIndex(k))
		}
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(time.Time{}) {
			v.Set(reflect.ValueOf(v.Interface().(time.Time).In(b.timezone)))
			return
		}

		for i := 0; i < v.NumField(); i++ {
			b.changeTimezone(v.Field(i))
		}
	}
}

func (b *selectBuilder) Build(d Dialect, buf Buffer) error {
	return b.selectStmt.Build(d, buf)
}

// Load loads any value from query result with background context
func (b *selectBuilder) Load(value interface{}) (int, error) {
	return b.LoadContext(context.Background(), value)
}

// LoadContext loads any value from query result
func (b *selectBuilder) LoadContext(ctx context.Context, value interface{}) (int, error) {
	c, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err == nil && b.timezone != nil {
		b.changeTimezone(reflect.ValueOf(value))
	}
	return c, err
}

// LoadStruct loads struct from query result with background context, returns ErrNotFound if there is no result
func (b *selectBuilder) LoadStruct(value interface{}) error {
	return b.LoadStructContext(context.Background(), value)
}

// LoadStructContext loads struct from query result, returns ErrNotFound if there is no result
func (b *selectBuilder) LoadStructContext(ctx context.Context, value interface{}) error {
	count, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	if b.timezone != nil {
		b.changeTimezone(reflect.ValueOf(value))
	}
	return nil
}

// LoadStructs loads structures from query result with background context
func (b *selectBuilder) LoadStructs(value interface{}) (int, error) {
	return b.LoadStructsContext(context.Background(), value)
}

// LoadStructsContext loads structures from query result
func (b *selectBuilder) LoadStructsContext(ctx context.Context, value interface{}) (int, error) {
	c, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err == nil && b.timezone != nil {
		b.changeTimezone(reflect.ValueOf(value))
	}
	return c, err
}

// GetRows returns sql.Rows from query result.
func (b *selectBuilder) GetRows(ctx context.Context) (*sql.Rows, error) {
	rows, _, err := queryRows(ctx, b.runner, b.EventReceiver, b, b.Dialect)

	return rows, err
}

// LoadValue loads any value from query result with background context, returns ErrNotFound if there is no result
func (b *selectBuilder) LoadValue(value interface{}) error {
	return b.LoadValueContext(context.Background(), value)
}

// LoadValueContext loads any value from query result, returns ErrNotFound if there is no result
func (b *selectBuilder) LoadValueContext(ctx context.Context, value interface{}) error {
	count, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	if b.timezone != nil {
		b.changeTimezone(reflect.ValueOf(value))
	}
	return nil
}

// LoadValues loads any values from query result with background context
func (b *selectBuilder) LoadValues(value interface{}) (int, error) {
	return b.LoadValuesContext(context.Background(), value)
}

// LoadValuesContext loads any values from query result
func (b *selectBuilder) LoadValuesContext(ctx context.Context, value interface{}) (int, error) {
	c, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err == nil && b.timezone != nil {
		b.changeTimezone(reflect.ValueOf(value))
	}
	return c, err
}

// Join joins table on condition
func (b *selectBuilder) Join(table, on interface{}) SelectBuilder {
	b.selectStmt.Join(table, on)
	return b
}

// LeftJoin joins table on condition via LEFT JOIN
func (b *selectBuilder) LeftJoin(table, on interface{}) SelectBuilder {
	b.selectStmt.LeftJoin(table, on)
	return b
}

// RightJoin joins table on condition via RIGHT JOIN
func (b *selectBuilder) RightJoin(table, on interface{}) SelectBuilder {
	b.selectStmt.RightJoin(table, on)
	return b
}

// FullJoin joins table on condition via FULL JOIN
func (b *selectBuilder) FullJoin(table, on interface{}) SelectBuilder {
	b.selectStmt.FullJoin(table, on)
	return b
}

// Distinct adds `DISTINCT`
func (b *selectBuilder) Distinct() SelectBuilder {
	b.selectStmt.Distinct()
	return b
}

// From specifies table
func (b *selectBuilder) From(table interface{}) SelectBuilder {
	b.selectStmt.From(table)
	return b
}

// GroupBy specifies columns for grouping
func (b *selectBuilder) GroupBy(col ...string) SelectBuilder {
	b.selectStmt.GroupBy(col...)
	return b
}

// Having adds a having condition
func (b *selectBuilder) Having(query interface{}, value ...interface{}) SelectBuilder {
	b.selectStmt.Having(query, value...)
	return b
}

// Limit adds LIMIT
func (b *selectBuilder) Limit(n uint64) SelectBuilder {
	b.selectStmt.Limit(n)
	return b
}

// Offset adds OFFSET, works only if LIMIT is set
func (b *selectBuilder) Offset(n uint64) SelectBuilder {
	b.selectStmt.Offset(n)
	return b
}

// OrderDir specifies columns for ordering in direction
func (b *selectBuilder) OrderDir(col string, isAsc bool) SelectBuilder {
	if isAsc {
		b.selectStmt.OrderAsc(col)
	} else {
		b.selectStmt.OrderDesc(col)
	}
	return b
}

// Paginate adds LIMIT and OFFSET
func (b *selectBuilder) Paginate(page, perPage uint64) SelectBuilder {
	b.Limit(perPage)
	b.Offset((page - 1) * perPage)
	return b
}

// OrderBy specifies column for ordering
func (b *selectBuilder) OrderBy(col string) SelectBuilder {
	b.selectStmt.Order = append(b.selectStmt.Order, Expr(col))
	return b
}

// Where adds a where condition
func (b *selectBuilder) Prewhere(query interface{}, value ...interface{}) SelectBuilder {
	b.selectStmt.Prewhere(query, value...)
	return b
}

// Where adds a where condition
func (b *selectBuilder) Where(query interface{}, value ...interface{}) SelectBuilder {
	b.selectStmt.Where(query, value...)
	return b
}

// ForUpdate adds lock via FOR UPDATE
func (b *selectBuilder) ForUpdate() SelectBuilder {
	b.selectStmt.ForUpdate()
	return b
}

// SkipLocked skips locked rows via SKIP LOCKED
func (b *selectBuilder) SkipLocked() SelectBuilder {
	b.selectStmt.SkipLocked()
	return b
}

// InTimezone all time.Time fields in the result will be returned with the specified location.
func (b *selectBuilder) InTimezone(loc *time.Location) SelectBuilder {
	b.timezone = loc
	return b
}

func (b *selectBuilder) OrderAsc(col string) SelectBuilder {
	b.selectStmt.OrderAsc(col)
	return b
}

func (b *selectBuilder) OrderDesc(col string) SelectBuilder {
	b.selectStmt.OrderDesc(col)
	return b
}

// As creates alias for select statement
func (b *selectBuilder) As(alias string) Builder {
	return b.selectStmt.As(alias)
}

// Comment adds a comment at the beginning of the query
func (b *selectBuilder) Comment(text string) SelectBuilder {
	b.selectStmt.AddComment(text)
	return b
}
