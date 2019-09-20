package dbr

import (
	"fmt"
	"strings"
)

type joinType uint8

const (
	inner joinType = iota
	left
	right
	full
)

func aliasTable(table, seperator string, d Dialect, buf Buffer) error {
	parts := strings.Split(table, seperator)
	if len(parts) != 2 {
		return fmt.Errorf("invalid table alias: '%s'", table)
	}
	tableName := strings.TrimSpace(parts[0])
	aliasName := strings.TrimSpace(parts[1])
	_, err := buf.WriteString(d.QuoteIdent(tableName) + " AS " + d.QuoteIdent(aliasName))
	return err
}

func join(t joinType, table, on interface{}) Builder {
	return BuildFunc(func(d Dialect, buf Buffer) error {
		buf.WriteString(" ")
		switch t {
		case left:
			buf.WriteString("LEFT ")
		case right:
			buf.WriteString("RIGHT ")
		case full:
			buf.WriteString("FULL ")
		}
		buf.WriteString("JOIN ")
		switch table := table.(type) {
		case string:
			table = strings.Replace(table, " AS ", " as ", 1)
			if strings.Contains(table, " as ") {
				aliasTable(table, " as ", d, buf)
			} else if strings.Contains(table, " ") {
				aliasTable(table, " ", d, buf)

			} else {
				buf.WriteString(d.QuoteIdent(table))
			}
		default:
			buf.WriteString(placeholder)
			buf.WriteValue(table)
		}
		buf.WriteString(" ON ")
		switch on := on.(type) {
		case string:
			buf.WriteString(on)
		case Builder:
			buf.WriteString(placeholder)
			buf.WriteValue(on)
		}
		return nil
	})
}
