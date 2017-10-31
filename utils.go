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

// DefaultTypeMapper is the default type mapper.
func DefaultTypeMapper(t reflect.Type) (out string, ok bool) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	ok = true
	switch t.Kind() {
	case reflect.String:
		out = "VARCHAR(255) NOT NULL"
	case reflect.Int, reflect.Int64:
		out = "BIGINT NOT NULL"
	case reflect.Uint, reflect.Uint64:
		out = "BIGINT UNSIGNED NOT NULL"
	case reflect.Int32:
		out = "INT NOT NULL"
	case reflect.Uint32:
		out = "INT UNSIGNED NOT NULL"
	case reflect.Int16:
		out = "SMALLINT NOT NULL"
	case reflect.Uint16:
		out = "SMALLINT UNSIGNED NOT NULL"
	case reflect.Int8:
		out = "TINYINT NOT NULL"
	case reflect.Uint8:
		out = "TINYINT UNSIGNED NOT NULL"
	case reflect.Float64:
		out = "DOUBLE NOT NULL"
	case reflect.Float32:
		out = "FLOAT NOT NULL"
	case reflect.Bool:
		out = "BIT NOT NULL"

	default:
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
			out = "BIT NULL"

		default:
			ok = false
		}
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
