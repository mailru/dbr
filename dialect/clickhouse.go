package dialect

import (
	"bytes"
	"fmt"
	"time"
)

const (
	clickhouseTimeFormat = "2006-01-02 15:04:05"
)

type clickhouse struct{}

func (d clickhouse) QuoteIdent(s string) string {
	return quoteIdent(s, "`")
}

func (d clickhouse) EncodeString(s string) string {
	buf := new(bytes.Buffer)

	buf.WriteRune('\'')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 0:
			buf.WriteString(`\0`)
		case '\'':
			buf.WriteString(`\'`)
		case '"':
			buf.WriteString(`\"`)
		case '\b':
			buf.WriteString(`\b`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case 26:
			buf.WriteString(`\Z`)
		case '\\':
			buf.WriteString(`\\`)
		default:
			buf.WriteByte(s[i])
		}
	}

	buf.WriteRune('\'')
	return buf.String()
}

func (d clickhouse) EncodeBool(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (d clickhouse) EncodeTime(t time.Time) string {
	return `'` + t.UTC().Format(clickhouseTimeFormat) + `'`
}

func (d clickhouse) EncodeBytes(b []byte) string {
	return fmt.Sprintf(`0x%x`, b)
}

func (d clickhouse) Placeholder(_ int) string {
	return "?"
}

func (d clickhouse) OnConflict(_ string) string {
	return ""
}

func (d clickhouse) Proposed(_ string) string {
	return ""
}

func (d clickhouse) Limit(offset, limit int64) string {
	if offset < 0 {
		return fmt.Sprintf("LIMIT %d", limit)
	}
	return fmt.Sprintf("LIMIT %d,%d", offset, limit)
}

func (d clickhouse) String() string {
	return "clickhouse"
}

func (d clickhouse) Prewhere() string {
	return "PREWHERE"
}
