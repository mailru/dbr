package dbr

import (
	"database/sql"
	"fmt"
)

// DeleteBuilder builds "DELETE ..." stmt
type DeleteBuilder interface {
	Builder
	EventReceiver
	Executer

	Where(query interface{}, value ...interface{}) DeleteBuilder
	Limit(n uint64) DeleteBuilder
}

type deleteBuilder struct {
	runner
	EventReceiver

	Dialect    Dialect
	deleteStmt *deleteStmt
	LimitCount int64
}

// DeleteFrom creates a DeleteBuilder
func (sess *Session) DeleteFrom(table string) DeleteBuilder {
	return &deleteBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		deleteStmt:    createDeleteStmt(table),
		LimitCount:    -1,
	}
}

// DeleteFrom creates a DeleteBuilder
func (tx *Tx) DeleteFrom(table string) DeleteBuilder {
	return &deleteBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		deleteStmt:    createDeleteStmt(table),
		LimitCount:    -1,
	}
}

// DeleteBySql creates a DeleteBuilder from raw query
func (sess *Session) DeleteBySql(query string, value ...interface{}) DeleteBuilder {
	return &deleteBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		deleteStmt:    createDeleteStmtBySQL(query, value),
		LimitCount:    -1,
	}
}

// DeleteBySql creates a DeleteBuilder from raw query
func (tx *Tx) DeleteBySql(query string, value ...interface{}) DeleteBuilder {
	return &deleteBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		deleteStmt:    createDeleteStmtBySQL(query, value),
		LimitCount:    -1,
	}
}

// Exec executes the stmt
func (b *deleteBuilder) Exec() (sql.Result, error) {
	return exec(b.runner, b.EventReceiver, b, b.Dialect)
}

// Where adds condition to the stmt
func (b *deleteBuilder) Where(query interface{}, value ...interface{}) DeleteBuilder {
	b.deleteStmt.Where(query, value...)
	return b
}

// Limit adds LIMIT
func (b *deleteBuilder) Limit(n uint64) DeleteBuilder {
	b.LimitCount = int64(n)
	return b
}

// Build builds `DELETE ...` in dialect
func (b *deleteBuilder) Build(d Dialect, buf Buffer) error {
	err := b.deleteStmt.Build(b.Dialect, buf)
	if err != nil {
		return err
	}
	if b.LimitCount >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(fmt.Sprint(b.LimitCount))
	}
	return nil
}
