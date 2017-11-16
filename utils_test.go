package  querier

import (
	"reflect"
	"testing"
)

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
		inDialect        Dialect
		wantName         string
		wantDataType     string
		wantIgnore       bool
		wantInlineStruct bool
	}{
		{model.Field(0), nil, "ID", "", false, false},
		{model.Field(1), Default{}, "Username", "VARCHAR(255) NOT NULL", false, false},
		{model.Field(2), nil, "age", "", false, false},
		{model.Field(3), nil, "Nationality", "VARCHAR(255) NULL", false, false},
		{model.Field(4), nil, "other", "INT", false, false},
		{model.Field(5), nil, "Complex", "", false, true},
		{model.Field(6), nil, "-", "", true, false},
	}

	for _, tt := range tests {
		gotName, gotDataType, gotIgnore, gotInlineStruct := extractFieldInfo(&tt.inField, tt.inDialect)
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
