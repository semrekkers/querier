package sugar

import (
	"database/sql"
	"reflect"
	"strings"
)

const (
	structFieldTagKey = "db"
)

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
func extractFieldInfo(field *reflect.StructField, d Dialect) (name, dataType string, ignore, inlineStruct bool) {
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
		if d != nil {
			var ok bool
			dataType, ok = d.TypeMapper(field.Type)
			if !ok {
				panic("invalid type of struct field")
			}
		}
	}

	return
}
