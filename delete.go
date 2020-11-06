package dbr

// DeleteStmt builds `DELETE ...`
type DeleteStmt interface {
	Builder
	Where(query interface{}, value ...interface{}) DeleteStmt
}

type deleteStmt struct {
	raw

	Table     string
	WhereCond []Builder
}

// Build builds `DELETE ...` in dialect
func (b *deleteStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	buf.WriteString("DELETE FROM ")
	buf.WriteString(d.QuoteIdent(b.Table))

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteFrom creates a DeleteStmt
func DeleteFrom(table string) DeleteStmt {
	return createDeleteStmt(table)
}

func createDeleteStmt(table string) *deleteStmt {
	return &deleteStmt{
		Table: table,
	}
}

// DeleteBySql creates a DeleteStmt from raw query
func DeleteBySql(query string, value ...interface{}) DeleteStmt {
	return createDeleteStmtBySQL(query, value)
}

func createDeleteStmtBySQL(query string, value []interface{}) *deleteStmt {
	return &deleteStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
	}
}

// Where adds a where condition
func (b *deleteStmt) Where(query interface{}, value ...interface{}) DeleteStmt {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}
