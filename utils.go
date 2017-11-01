package sugar

import (
	"database/sql"
	"reflect"
	"strings"
	"time"
)

const (
	structFieldTagKey = "db"
)

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

// DefaultTypeMapper is the default type mapper.
func DefaultTypeMapper(t reflect.Type) (out string, ok bool) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if out, ok = typeMap[t.Kind()]; ok {
		return
	}

	ok = true
	switch t {
	case reflectTypeByteSlice:
		out = "VARBINARY(255) NULL"
	case reflectTypeTime:
		out = "DATETIME NOT NULL"
	case reflectTypeNullString:
		out = "VARCHAR(255) NULL"
	case reflectTypeNullInt64:
		out = "BIGINT NULL"
	case reflectTypeNullFloat64:
		out = "DOUBLE NULL"
	case reflectTypeNullBool:
		out = "BOOLEAN NULL"
	default:
		ok = false
	}

	return
}

// DefaultBindVar is the default bindvar formatter.
func DefaultBindVar(*Querier, int) string {
	return "?"
}

// AppendToStringSlice returns a ScanFunc that will append any result of the first column of the query to slice s. Panics when s is invalid.
func AppendToStringSlice(s *[]string) ScanFunc {
	if s == nil {
		panic("invalid string slice")
	}
	return func(_ *Querier, r *sql.Rows) error {
		var str string
		if err := r.Scan(&str); err != nil {
			return err
		}
		*s = append(*s, str)
		return nil
	}
}

// extractField returns info about a StructField.
func extractFieldInfo(field *reflect.StructField, mapper TypeMapper) (name, dataType string, ignore, inlineStruct bool) {
	name = field.Name
	tagParts := strings.SplitN(field.Tag.Get(structFieldTagKey), ",", 2)

	if tagParts[0] != "" {
		// A field name is set in the field tag, use this as the field name.
		name = tagParts[0]
	}
	if name == "-" {
		// Name equals "-", ignore this field.
		ignore = true
		return
	}

	if len(tagParts) == 2 {
		// A datatype is set in the field tag, use this as the field's data type.
		dataType = tagParts[1]
	}
	if dataType == "" {
		if field.Type.Kind() == reflect.Struct {
			receiver := reflect.PtrTo(field.Type)
			if field.Type != reflectTypeTime && !receiver.Implements(reflectTypeScanner) {
				// This field is an inline struct.
				inlineStruct = true
				return
			}
		}
		if mapper != nil {
			var ok bool
			dataType, ok = mapper(field.Type)
			if !ok {
				panic("invalid type of struct field")
			}
		}
	}

	return
}

func extractStructSliceInfo(i interface{}) (v reflect.Value, elemType reflect.Type, elemIsPtr bool) {
	v = reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		panic("argument i is not a pointer")
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		panic("argument i is not a pointer to a slice")
	}
	elemType = v.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		elemIsPtr = true
	}
	if elemType.Kind() != reflect.Struct {
		panic("argument i is not a slice of (pointers to) structs")
	}
	return
}
