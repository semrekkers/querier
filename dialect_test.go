package sugar

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func TestDefaultTypeMapper(t *testing.T) {
	var (
		str string
	)

	tests := []struct {
		in      interface{}
		wantOut string
		wantOk  bool
	}{
		{"", "VARCHAR(255) NOT NULL", true},
		{&str, "VARCHAR(255) NOT NULL", true},

		{int(0), "BIGINT NOT NULL", true},
		{int64(0), "BIGINT NOT NULL", true},
		{int32(0), "INT NOT NULL", true},
		{int16(0), "SMALLINT NOT NULL", true},
		{int8(0), "TINYINT NOT NULL", true},

		{uint(0), "BIGINT UNSIGNED NOT NULL", true},
		{uint64(0), "BIGINT UNSIGNED NOT NULL", true},
		{uint32(0), "INT UNSIGNED NOT NULL", true},
		{uint16(0), "SMALLINT UNSIGNED NOT NULL", true},
		{uint8(0), "TINYINT UNSIGNED NOT NULL", true},

		{float32(0), "FLOAT NOT NULL", true},
		{float64(0), "DOUBLE NOT NULL", true},

		{false, "BOOLEAN NOT NULL", true},

		{[]byte{}, "VARBINARY(255) NULL", true},
		{time.Time{}, "DATETIME NOT NULL", true},
		{sql.NullString{}, "VARCHAR(255) NULL", true},
		{sql.NullInt64{}, "BIGINT NULL", true},
		{sql.NullFloat64{}, "DOUBLE NULL", true},
		{sql.NullBool{}, "BOOLEAN NULL", true},

		{reflect.Value{}, "", false},
	}

	for _, tt := range tests {
		gotOut, gotOk := Default{}.TypeMapper(reflect.TypeOf(tt.in))
		if gotOut != tt.wantOut {
			t.Errorf("TypeMapper(%T) out = %q, want %q", tt.in, gotOut, tt.wantOut)
		}
		if gotOk != tt.wantOk {
			t.Errorf("TypeMapper(%T) ok = %t, want %t", tt.in, gotOk, tt.wantOk)
		}
	}
}
