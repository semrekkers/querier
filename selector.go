package  querier

import (
	"reflect"
)

// Field represents a mapped field.
type Field struct {
	// Name is the field's name.
	Name string
	// DataType is the field's data type.
	DataType string
}

// FieldSelector selects struct fields from a struct type and builds a Field slice.
type FieldSelector struct {
	t reflect.Type
	d Dialect

	filterSet     map[string]struct{}
	filterExclude bool
}

// Fields returns a new FieldSelector with i as base struct. Fields panics when i is non-struct.
func Fields(i interface{}) *FieldSelector {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		// Use the element type of the pointer.
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("argument i is not a struct")
	}
	return &FieldSelector{
		t: t,
		d: Default{},
	}
}

func (s *FieldSelector) Only(fields ...string) *FieldSelector {
	if s.filterSet != nil {
		if s.filterExclude {
			panic("an Except filter was already set")
		}
	} else {
		s.filterSet = make(map[string]struct{})
	}
	for _, field := range fields {
		s.filterSet[field] = struct{}{}
	}
	return s
}

func (s *FieldSelector) Except(fields ...string) *FieldSelector {
	if s.filterSet != nil {
		if !s.filterExclude {
			panic("an Only filter was already set")
		}
	} else {
		s.filterSet = make(map[string]struct{})
		s.filterExclude = true
	}
	for _, field := range fields {
		s.filterSet[field] = struct{}{}
	}
	return s
}

// SetDialect sets the Dialect for this FieldSelector.
func (s *FieldSelector) SetDialect(d Dialect) *FieldSelector {
	s.d = d
	return s
}

func (s *FieldSelector) Select() []Field {
	return makeFieldSlice(s.t, nil, s.d, s.filterSet, s.filterExclude)
}

type ValueMap map[string]reflect.Value

func Values(i interface{}) ValueMap {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		panic("argument i is not a pointer")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		panic("argument i is not a pointer to a struct")
	}
	return makeValueMap(v, nil)
}

var ignore interface{}

func (m ValueMap) MapToColumns(columns []string, values []interface{}) []interface{} {
	if values == nil {
		// This is an optimization because we know what the length of values will be.
		values = make([]interface{}, 0, len(columns))
	}
	for _, column := range columns {
		var v interface{}
		if value, ok := m[column]; ok {
			v = value.Addr().Interface()
		} else {
			v = &ignore
		}
		values = append(values, v)
	}
	return values
}

func (m ValueMap) MapToFields(fields []Field, values []interface{}) []interface{} {
	if values == nil {
		// This is an optimization because we know what the length of values will be.
		values = make([]interface{}, 0, len(fields))
	}
	for _, field := range fields {
		var v interface{}
		if value, ok := m[field.Name]; ok {
			v = value.Addr().Interface()
		} else {
			v = &ignore
		}
		values = append(values, v)
	}
	return values
}

func makeFieldSlice(t reflect.Type, fields []Field, d Dialect, filterSet map[string]struct{}, filterExclude bool) []Field {
	numField := t.NumField()
	if fields == nil {
		fields = make([]Field, 0, numField)
	}

	for i := 0; i < numField; i++ {
		cur := t.Field(i)
		name, dataType, ignore, inlineStruct := extractFieldInfo(&cur, d)

		if ignore {
			// Skip this field.
			continue
		}
		if inlineStruct {
			// Flatten this inline struct.
			fields = makeFieldSlice(cur.Type, fields, d, filterSet, filterExclude)
			continue
		}
		if filterSet != nil {
			_, inSet := filterSet[name]
			// If filter mode is include and field is in set then procceed.
			// If filter mode is include and field is not in set then skip.
			// If filter mode is exclude and field is in set then skip.
			// If filter mode is exclude and field is not in set then procceed.
			if (!inSet && !filterExclude) || (inSet && filterExclude) {
				// Skip this field.
				continue
			}
		}

		fields = append(fields, Field{
			Name:     name,
			DataType: dataType,
		})
	}

	return fields
}

func makeValueMap(v reflect.Value, values ValueMap) ValueMap {
	t := v.Type()
	numField := t.NumField()
	if values == nil {
		values = make(ValueMap)
	}

	for i := 0; i < numField; i++ {
		cur := t.Field(i)
		name, _, ignore, inlineStruct := extractFieldInfo(&cur, nil)

		if ignore {
			// Skip this field.
			continue
		}

		fieldValue := v.Field(i)
		if inlineStruct {
			// Flatten this inline struct.
			makeValueMap(fieldValue, values)
		} else {
			values[name] = fieldValue
		}
	}

	return values
}
