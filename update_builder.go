package dbr

import (
	"database/sql"
	"fmt"
)

// UpdateBuilder builds `UPDATE ...`
type UpdateBuilder interface {
	Builder
	EventReceiver
	Executer

	Where(query interface{}, value ...interface{}) UpdateBuilder
	Set(column string, value interface{}) UpdateBuilder
	SetMap(m map[string]interface{}) UpdateBuilder
	Limit(n uint64) UpdateBuilder
}

type updateBuilder struct {
	EventReceiver
	runner

	Dialect    Dialect
	updateStmt *updateStmt
	LimitCount int64
}

// Update creates a UpdateBuilder
func (sess *Session) Update(table string) UpdateBuilder {
	return &updateBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		updateStmt:    createUpdateStmt(table),
		LimitCount:    -1,
	}
}

// Update creates a UpdateBuilder
func (tx *Tx) Update(table string) UpdateBuilder {
	return &updateBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		updateStmt:    createUpdateStmt(table),
		LimitCount:    -1,
	}
}

// UpdateBySql creates a UpdateBuilder from raw query
func (sess *Session) UpdateBySql(query string, value ...interface{}) UpdateBuilder {
	return &updateBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		updateStmt:    createUpdateStmtBySQL(query, value),
		LimitCount:    -1,
	}
}

// UpdateBySql creates a UpdateBuilder from raw query
func (tx *Tx) UpdateBySql(query string, value ...interface{}) UpdateBuilder {
	return &updateBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		updateStmt:    createUpdateStmtBySQL(query, value),
		LimitCount:    -1,
	}
}

// Exec executes the stmt
func (b *updateBuilder) Exec() (sql.Result, error) {
	return exec(b.runner, b.EventReceiver, b, b.Dialect)
}

// Set adds "SET column=value"
func (b *updateBuilder) Set(column string, value interface{}) UpdateBuilder {
	b.updateStmt.Set(column, value)
	return b
}

// SetMap adds "SET column=value" for each key value pair in m
func (b *updateBuilder) SetMap(m map[string]interface{}) UpdateBuilder {
	b.updateStmt.SetMap(m)
	return b
}

// Where adds condition to the stmt
func (b *updateBuilder) Where(query interface{}, value ...interface{}) UpdateBuilder {
	b.updateStmt.Where(query, value...)
	return b
}

// Limit adds LIMIT
func (b *updateBuilder) Limit(n uint64) UpdateBuilder {
	b.LimitCount = int64(n)
	return b
}

// Build builds `UPDATE ...` in dialect
func (b *updateBuilder) Build(d Dialect, buf Buffer) error {
	err := b.updateStmt.Build(b.Dialect, buf)
	if err != nil {
		return err
	}
	if b.LimitCount >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(fmt.Sprint(b.LimitCount))
	}
	return nil
}
