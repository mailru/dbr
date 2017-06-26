// +build go1.8

package dbr

import (
	"context"
	"testing"

	"github.com/mailru/dbr/dialect"

	"database/sql"
	"github.com/stretchr/testify/assert"
)

func TestContextCancel(t *testing.T) {
	// context support is implemented for PostgreSQL
	for _, sess := range testSession {
		if sess.Dialect == dialect.SQLite3 {
			continue
		}
		checkSessionContext(t, postgresSession.Connection)
		if sess.Dialect != dialect.ClickHouse {
			checkTxQueryContext(t, postgresSession.Connection)
			checkTxExecContext(t, postgresSession.Connection)
		}
	}
}

func checkSessionContext(t *testing.T, conn *Connection) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sess := conn.NewSessionContext(ctx, nil)
	_, err := sess.SelectBySql("SELECT 1").ReturnInt64()
	assert.EqualError(t, err, "context canceled")
	_, err = sess.Update("dbr_people").Where(Eq("id", 1)).Set("name", "jonathan1").Exec()
	assert.EqualError(t, err, "context canceled")
	_, err = sess.Begin()
	assert.EqualError(t, err, "context canceled")
}

func checkTxQueryContext(t *testing.T, conn *Connection) {
	ctx, cancel := context.WithCancel(context.Background())
	sess := conn.NewSessionContext(ctx, nil)
	tx, err := sess.Begin()
	if !assert.NoError(t, err) {
		cancel()
		return
	}
	cancel()
	_, err = tx.SelectBySql("SELECT 1").ReturnInt64()
	assert.EqualError(t, err, "context canceled")
	err = tx.Rollback()
	// context cancel may cause transaction rollback automatically
	assert.True(t, err == nil || err == sql.ErrTxDone)
}

func checkTxExecContext(t *testing.T, conn *Connection) {
	ctx, cancel := context.WithCancel(context.Background())
	sess := conn.NewSessionContext(ctx, nil)
	tx, err := sess.Begin()
	if !assert.NoError(t, err) {
		cancel()
		return
	}
	_, err = tx.Update("dbr_people").Where(Eq("id", 1)).Set("name", "jonathan1").Exec()
	assert.NoError(t, err)
	cancel()
	assert.EqualError(t, tx.Commit(), "context canceled")
}
