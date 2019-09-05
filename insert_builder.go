package dbr

import (
	"database/sql"
	"reflect"
)

// InsertBuilder builds "INSERT ..." stmt
type InsertBuilder interface {
	Builder
	EventReceiver
	Executer
	Columns(column ...string) InsertBuilder
	Values(value ...interface{}) InsertBuilder
	Record(structValue interface{}) InsertBuilder
	OnConflictMap(constraint string, actions map[string]interface{}) InsertBuilder
	OnConflict(constraint string) ConflictStmt
	Pair(column string, value interface{}) InsertBuilder
}

// InsertBuilder builds "INSERT ..." stmt
type insertBuilder struct {
	EventReceiver
	runner

	Dialect    Dialect
	RecordID   reflect.Value
	insertStmt *insertStmt
}

// InsertInto creates a InsertBuilder
func (sess *Session) InsertInto(table string) InsertBuilder {
	return &insertBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		insertStmt:    createInsertStmt(table),
	}
}

// InsertInto creates a InsertBuilder
func (tx *Tx) InsertInto(table string) InsertBuilder {
	return &insertBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		insertStmt:    createInsertStmt(table),
	}
}

// InsertBySql creates a InsertBuilder from raw query
func (sess *Session) InsertBySql(query string, value ...interface{}) InsertBuilder {
	return &insertBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		insertStmt:    createInsertStmtBySQL(query, value),
	}
}

// InsertBySql creates a InsertBuilder from raw query
func (tx *Tx) InsertBySql(query string, value ...interface{}) InsertBuilder {
	return &insertBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		insertStmt:    createInsertStmtBySQL(query, value),
	}
}

func (b *insertBuilder) Build(d Dialect, buf Buffer) error {
	return b.insertStmt.Build(d, buf)
}

// Pair adds a new column value pair
func (b *insertBuilder) Pair(column string, value interface{}) InsertBuilder {
	b.Columns(column)
	switch len(b.insertStmt.Value) {
	case 0:
		b.insertStmt.Values(value)
	case 1:
		b.insertStmt.Value[0] = append(b.insertStmt.Value[0], value)
	default:
		panic("pair only allows one record to insert")
	}
	return b
}

// Exec executes the stmt
func (b *insertBuilder) Exec() (sql.Result, error) {
	result, err := exec(b.runner, b.EventReceiver, b, b.Dialect)
	if err != nil {
		return nil, err
	}

	if b.RecordID.IsValid() {
		if id, err := result.LastInsertId(); err == nil {
			b.RecordID.SetInt(id)
		}
	}

	return result, nil
}

// Columns adds columns
func (b *insertBuilder) Columns(column ...string) InsertBuilder {
	b.insertStmt.Columns(column...)
	return b
}

// Values adds a tuple for columns
func (b *insertBuilder) Values(value ...interface{}) InsertBuilder {
	b.insertStmt.Values(value...)
	return b
}

// Record adds a tuple for columns from a struct
func (b *insertBuilder) Record(structValue interface{}) InsertBuilder {
	v := reflect.Indirect(reflect.ValueOf(structValue))
	if v.Kind() == reflect.Struct && v.CanSet() {
		// ID is recommended by golint here
		for _, name := range []string{"Id", "ID"} {
			field := v.FieldByName(name)
			if field.IsValid() && field.Kind() == reflect.Int64 {
				b.RecordID = field
				break
			}
		}
	}

	b.insertStmt.Record(structValue)
	return b
}

// OnConflictMap allows to add actions for constraint violation, e.g UPSERT
func (b *insertBuilder) OnConflictMap(constraint string, actions map[string]interface{}) InsertBuilder {
	b.insertStmt.OnConflictMap(constraint, actions)
	return b
}

// OnConflict creates an empty OnConflict section fo insert statement , e.g UPSERT
func (b *insertBuilder) OnConflict(constraint string) ConflictStmt {
	return b.insertStmt.OnConflict(constraint)
}
