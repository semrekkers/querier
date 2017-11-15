package sugar

import (
	"database/sql"
	"reflect"
	"time"
)

// Dialect represents a SQL dialect.
type Dialect interface {
	// TypeMapper maps a Go type to a SQL data type. It returns true and a dataType whether the mapping succeeded.
	TypeMapper(t reflect.Type) (dataType string, ok bool)

	// BindVar returns a formatted bind variable. i represents the current iteration.
	BindVar(q *Querier, i int) string
}

// Default is the default SQL-dialect.
type Default struct{}

var (
	reflectTypeScanner     = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	reflectTypeByteSlice   = reflect.TypeOf([]byte{})
	reflectTypeTime        = reflect.TypeOf(time.Time{})
	reflectTypeNullString  = reflect.TypeOf(sql.NullString{})
	reflectTypeNullInt64   = reflect.TypeOf(sql.NullInt64{})
	reflectTypeNullFloat64 = reflect.TypeOf(sql.NullFloat64{})
	reflectTypeNullBool    = reflect.TypeOf(sql.NullBool{})
)

var typeMap = map[reflect.Kind]string{
	reflect.String:  "VARCHAR(255) NOT NULL",
	reflect.Int:     "BIGINT NOT NULL",
	reflect.Int64:   "BIGINT NOT NULL",
	reflect.Int32:   "INT NOT NULL",
	reflect.Int16:   "SMALLINT NOT NULL",
	reflect.Int8:    "TINYINT NOT NULL",
	reflect.Uint:    "BIGINT UNSIGNED NOT NULL",
	reflect.Uint64:  "BIGINT UNSIGNED NOT NULL",
	reflect.Uint32:  "INT UNSIGNED NOT NULL",
	reflect.Uint16:  "SMALLINT UNSIGNED NOT NULL",
	reflect.Uint8:   "TINYINT UNSIGNED NOT NULL",
	reflect.Float64: "DOUBLE NOT NULL",
	reflect.Float32: "FLOAT NOT NULL",
	reflect.Bool:    "BOOLEAN NOT NULL",
}

// TypeMapper is the default type mapper.
func (Default) TypeMapper(t reflect.Type) (dataType string, ok bool) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if dataType, ok = typeMap[t.Kind()]; ok {
		return
	}

	ok = true
	switch t {
	case reflectTypeByteSlice:
		dataType = "VARBINARY(255) NULL"
	case reflectTypeTime:
		dataType = "DATETIME NOT NULL"
	case reflectTypeNullString:
		dataType = "VARCHAR(255) NULL"
	case reflectTypeNullInt64:
		dataType = "BIGINT NULL"
	case reflectTypeNullFloat64:
		dataType = "DOUBLE NULL"
	case reflectTypeNullBool:
		dataType = "BOOLEAN NULL"
	default:
		ok = false
	}

	return
}

// BindVar returns the default bindvar.
func (Default) BindVar(*Querier, int) string {
	return "?"
}
