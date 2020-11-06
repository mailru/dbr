package dbr

// SelectStmt builds `SELECT ...`
type SelectStmt interface {
	Builder

	From(table interface{}) SelectStmt
	Distinct() SelectStmt
	Prewhere(query interface{}, value ...interface{}) SelectStmt
	Where(query interface{}, value ...interface{}) SelectStmt
	Having(query interface{}, value ...interface{}) SelectStmt
	GroupBy(col ...string) SelectStmt
	OrderAsc(col string) SelectStmt
	OrderDesc(col string) SelectStmt
	Limit(n uint64) SelectStmt
	Offset(n uint64) SelectStmt
	ForUpdate() SelectStmt
	Join(table, on interface{}) SelectStmt
	LeftJoin(table, on interface{}) SelectStmt
	RightJoin(table, on interface{}) SelectStmt
	FullJoin(table, on interface{}) SelectStmt
	As(alias string) Builder
}

type selectStmt struct {
	raw

	IsDistinct bool

	Column    []interface{}
	Table     interface{}
	JoinTable []Builder

	PrewhereCond []Builder
	WhereCond    []Builder
	Group        []Builder
	HavingCond   []Builder
	Order        []Builder

	LimitCount  int64
	OffsetCount int64
	IsForUpdate bool
}

// Build builds `SELECT ...` in dialect
func (b *selectStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if len(b.Column) == 0 {
		return ErrColumnNotSpecified
	}

	buf.WriteString("SELECT ")

	if b.IsDistinct {
		buf.WriteString("DISTINCT ")
	}

	for i, col := range b.Column {
		if i > 0 {
			buf.WriteString(", ")
		}
		switch col := col.(type) {
		case string:
			buf.WriteString(col)
		default:
			buf.WriteString(placeholder)
			buf.WriteValue(col)
		}
	}

	if b.Table != nil {
		buf.WriteString(" FROM ")
		switch table := b.Table.(type) {
		case string:
			buf.WriteString(table)
		default:
			buf.WriteString(placeholder)
			buf.WriteValue(table)
		}
		if len(b.JoinTable) > 0 {
			for _, join := range b.JoinTable {
				err := join.Build(d, buf)
				if err != nil {
					return err
				}
			}
		}
	}

	if len(b.PrewhereCond) > 0 {
		keyword := d.Prewhere()
		if len(keyword) == 0 {
			return ErrPrewhereNotSupported
		}

		buf.WriteString(" ")
		buf.WriteString(keyword)
		buf.WriteString(" ")
		err := And(b.PrewhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}

	if len(b.Group) > 0 {
		buf.WriteString(" GROUP BY ")
		for i, group := range b.Group {
			if i > 0 {
				buf.WriteString(", ")
			}
			err := group.Build(d, buf)
			if err != nil {
				return err
			}
		}
	}

	if len(b.HavingCond) > 0 {
		buf.WriteString(" HAVING ")
		err := And(b.HavingCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}

	if len(b.Order) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, order := range b.Order {
			if i > 0 {
				buf.WriteString(", ")
			}
			err := order.Build(d, buf)
			if err != nil {
				return err
			}
		}
	}

	if b.LimitCount >= 0 {
		buf.WriteString(" ")
		buf.WriteString(d.Limit(b.OffsetCount, b.LimitCount))
	}

	if b.IsForUpdate {
		buf.WriteString(" FOR UPDATE")
	}
	return nil
}

// Select creates a SelectStmt
func Select(column ...interface{}) SelectStmt {
	return createSelectStmt(column)
}

func createSelectStmt(column []interface{}) *selectStmt {
	return &selectStmt{
		Column:      column,
		LimitCount:  -1,
		OffsetCount: -1,
	}
}

// From specifies table
func (b *selectStmt) From(table interface{}) SelectStmt {
	b.Table = table
	return b
}

// SelectBySql creates a SelectStmt from raw query
func SelectBySql(query string, value ...interface{}) SelectStmt {
	return createSelectStmtBySQL(query, value)
}

func createSelectStmtBySQL(query string, value []interface{}) *selectStmt {
	return &selectStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
		LimitCount:  -1,
		OffsetCount: -1,
	}
}

// Distinct adds `DISTINCT`
func (b *selectStmt) Distinct() SelectStmt {
	b.IsDistinct = true
	return b
}

// Prewhere adds a prewhere condition
// For example clickhouse PREWHERE:
// https://clickhouse.yandex/docs/en/query_language/select/#prewhere-clause
func (b *selectStmt) Prewhere(query interface{}, value ...interface{}) SelectStmt {
	switch query := query.(type) {
	case string:
		b.PrewhereCond = append(b.PrewhereCond, Expr(query, value...))
	case Builder:
		b.PrewhereCond = append(b.PrewhereCond, query)
	}
	return b
}

// Where adds a where condition
func (b *selectStmt) Where(query interface{}, value ...interface{}) SelectStmt {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

// Having adds a having condition
func (b *selectStmt) Having(query interface{}, value ...interface{}) SelectStmt {
	switch query := query.(type) {
	case string:
		b.HavingCond = append(b.HavingCond, Expr(query, value...))
	case Builder:
		b.HavingCond = append(b.HavingCond, query)
	}
	return b
}

// GroupBy specifies columns for grouping
func (b *selectStmt) GroupBy(col ...string) SelectStmt {
	for _, group := range col {
		b.Group = append(b.Group, Expr(group))
	}
	return b
}

// OrderAsc specifies columns for ordering in asc direction
func (b *selectStmt) OrderAsc(col string) SelectStmt {
	b.Order = append(b.Order, order(col, asc))
	return b
}

// OrderDesc specifies columns for ordering in desc direction
func (b *selectStmt) OrderDesc(col string) SelectStmt {
	b.Order = append(b.Order, order(col, desc))
	return b
}

// Limit adds LIMIT
func (b *selectStmt) Limit(n uint64) SelectStmt {
	b.LimitCount = int64(n)
	return b
}

// Offset adds OFFSET, works only if LIMIT is set
func (b *selectStmt) Offset(n uint64) SelectStmt {
	b.OffsetCount = int64(n)
	return b
}

// ForUpdate adds `FOR UPDATE`
func (b *selectStmt) ForUpdate() SelectStmt {
	b.IsForUpdate = true
	return b
}

// Join joins table on condition
func (b *selectStmt) Join(table, on interface{}) SelectStmt {
	b.JoinTable = append(b.JoinTable, join(inner, table, on))
	return b
}

// LeftJoin joins table on condition via LEFT JOIN
func (b *selectStmt) LeftJoin(table, on interface{}) SelectStmt {
	b.JoinTable = append(b.JoinTable, join(left, table, on))
	return b
}

// RightJoin joins table on condition via RIGHT JOIN
func (b *selectStmt) RightJoin(table, on interface{}) SelectStmt {
	b.JoinTable = append(b.JoinTable, join(right, table, on))
	return b
}

// FullJoin joins table on condition via FULL JOIN
func (b *selectStmt) FullJoin(table, on interface{}) SelectStmt {
	b.JoinTable = append(b.JoinTable, join(full, table, on))
	return b
}

// As creates alias for select statement
func (b *selectStmt) As(alias string) Builder {
	return as(b, alias)
}
