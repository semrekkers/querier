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

		{false, "BIT NOT NULL", true},

		{[]byte{}, "VARBINARY(255) NULL", true},
		{time.Time{}, "DATETIME NOT NULL", true},
		{sql.NullString{}, "VARCHAR(255) NULL", true},
		{sql.NullInt64{}, "BIGINT NULL", true},
		{sql.NullFloat64{}, "DOUBLE NULL", true},
		{sql.NullBool{}, "BIT NULL", true},

		{reflect.Value{}, "", false},
	}

	for _, tt := range tests {
		gotOut, gotOk := DefaultTypeMapper(reflect.TypeOf(tt.in))
		if gotOut != tt.wantOut {
			t.Errorf("DefaultTypeMapper(%T) out = %q, want %q", tt.in, gotOut, tt.wantOut)
		}
		if gotOk != tt.wantOk {
			t.Errorf("DefaultTypeMapper(%T) ok = %t, want %t", tt.in, gotOk, tt.wantOk)
		}
	}
}

func TestExtractFieldInfo(t *testing.T) {
	model := reflect.TypeOf(struct {
		ID          int
		Username    string
		Age         int    `db:"age"`
		Nationality string `db:",VARCHAR(255) NULL"`
		Other       string `db:"other,INT"`
		Complex     struct{}
		Ignored     string `db:"-"`
	}{})

	tests := []struct {
		inField          reflect.StructField
		inMapper         TypeMapper
		wantName         string
		wantDataType     string
		wantIgnore       bool
		wantInlineStruct bool
	}{
		{model.Field(0), nil, "ID", "", false, false},
		{model.Field(1), DefaultTypeMapper, "Username", "VARCHAR(255) NOT NULL", false, false},
		{model.Field(2), nil, "age", "", false, false},
		{model.Field(3), nil, "Nationality", "VARCHAR(255) NULL", false, false},
		{model.Field(4), nil, "other", "INT", false, false},
		{model.Field(5), nil, "Complex", "", false, true},
		{model.Field(6), nil, "-", "", true, false},
	}

	for _, tt := range tests {
		gotName, gotDataType, gotIgnore, gotInlineStruct := extractFieldInfo(&tt.inField, tt.inMapper)
		if gotName != tt.wantName {
			t.Errorf("extractFieldInfo() name = %q, want %q", gotName, tt.wantName)
		}
		if gotDataType != tt.wantDataType {
			t.Errorf("extractFieldInfo() dataType = %q, want %q", gotDataType, tt.wantDataType)
		}
		if gotIgnore != tt.wantIgnore {
			t.Errorf("extractFieldInfo() ignore = %v, want %v", gotIgnore, tt.wantIgnore)
		}
		if gotInlineStruct != tt.wantInlineStruct {
			t.Errorf("extractFieldInfo() inlineStruct = %v, want %v", gotInlineStruct, tt.wantInlineStruct)
		}
	}
}
