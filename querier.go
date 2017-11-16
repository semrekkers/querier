// Package querier provides a simple SQL query builder and executor.
package querier

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

const (
	// Space is a single space.
	Space = " "
	// FieldSep is a single field separator.
	FieldSep = ", "
)

var (
	// ErrNoRecord means that the record was not found.
	ErrNoRecord = errors.New("no record found")

	errEmptyQuery = errors.New("query is empty")
)

// Executor is an interface for an opaque query executor.
type Executor interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// DeferFunc runs when the query is finished.
type DeferFunc func(*Q)

// ScanFunc is called for each row in the result set.
type ScanFunc func(*Q, *sql.Rows) error

// Q can build and execute queries.
type Q struct {
	ex Executor
	d  Dialect

	// Query builder
	query    bytes.Buffer
	sep      string
	preWrite string
	params   []interface{}

	// For deffered functions.
	err          error
	lastInsertID int64
	rowsAffected int64
	deferred     []DeferFunc
}

// New returns a new querier.
func New(ex Executor, d Dialect) *Q {
	return &Q{ex: ex, d: d, sep: Space}
}

// Write writes a string (query) to the Querier. A single space is appended after query.
func (q *Q) Write(query string, params ...interface{}) *Q {
	q.writeSep()
	q.query.WriteString(query)
	q.params = append(q.params, params...)
	return q
}

// Writef writes a formatted string (format) to the Querier. A single space is appended after query.
func (q *Q) Writef(format string, args ...interface{}) *Q {
	q.writeSep()
	q.query.WriteString(fmt.Sprintf(format, args...))
	return q
}

func (q *Q) WriteFields(format, sep string, fields ...Field) *Q {
	q.writeSep()
	q.writeFormat(format, sep, fields, len(fields))
	return q
}

func (q *Q) WriteValues(format, sep string, values ...interface{}) *Q {
	q.writeSep()
	q.writeFormat(format, sep, nil, len(values))
	q.params = append(q.params, values...)
	return q
}

func (q *Q) WriteValueMap(format, sep string, valueMap ValueMap, fields ...Field) *Q {
	q.writeSep()
	q.writeFormat(format, sep, fields, len(fields))
	q.params = append(q.params, valueMap.MapToFields(fields, nil)...)
	return q
}

func (q *Q) WriteRaw(s string) *Q {
	q.query.WriteString(s)
	return q
}

func (q *Q) Prepend(query string) *Q {
	var buf bytes.Buffer
	buf.WriteString(query)
	buf.WriteString(q.sep)
	q.query.WriteTo(&buf)
	q.query = buf
	return q
}

func (q *Q) PreWrite() *Q {
	if q.preWrite != "" {
		q.writeSep()
		q.query.WriteString(q.preWrite)
	}
	return q
}

func (q *Q) SetPreWrite(s string) *Q {
	q.preWrite = s
	return q
}

func (q *Q) SetSeparator(sep string) *Q {
	q.sep = sep
	return q
}

func (q *Q) SetDialect(d Dialect) *Q {
	q.d = d
	return q
}

func (q *Q) AddParams(params ...interface{}) *Q {
	q.params = append(q.params, params...)
	return q
}

func (q *Q) Params() []interface{} {
	return q.params
}

func (q *Q) String() string {
	return q.query.String()
}

func (q *Q) Defer(fn DeferFunc) *Q {
	q.deferred = append(q.deferred, fn)
	return q
}

func (q *Q) DeferSuccess(fn DeferFunc) *Q {
	q.deferred = append(q.deferred, func(q *Q) {
		if q.err == nil {
			fn(q)
		}
	})
	return q
}

func (q *Q) ExecContext(ctx context.Context) error {
	if q.query.Len() == 0 {
		panic(errEmptyQuery)
	}
	defer q.runDeferred()

	result, err := q.ex.ExecContext(ctx, q.query.String(), q.params...)
	if err != nil {
		return q.returnErr(err)
	}
	if q.rowsAffected, err = result.RowsAffected(); err != nil {
		return q.returnErr(err)
	}
	q.lastInsertID, err = result.LastInsertId()

	return q.returnErr(err)
}

func (q *Q) Exec() error {
	return q.ExecContext(context.Background())
}

func (q *Q) FirstContext(ctx context.Context, i interface{}) error {
	if q.query.Len() == 0 {
		panic(errEmptyQuery)
	}
	valueMap := Values(i)
	defer q.runDeferred()

	rows, err := q.ex.QueryContext(ctx, q.query.String(), q.params...)
	if err != nil {
		return q.returnErr(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return q.returnErr(ErrNoRecord)
	}
	columns, err := rows.Columns()
	if err != nil {
		return q.returnErr(err)
	}
	err = rows.Scan(valueMap.MapToColumns(columns, nil)...)
	return q.returnErr(err)
}

func (q *Q) First(i interface{}) error {
	return q.FirstContext(context.Background(), i)
}

func (q *Q) FindContext(ctx context.Context, i interface{}) error {
	if q.query.Len() == 0 {
		panic(errEmptyQuery)
	}
	v, elemType, elemIsPtr := extractStructSliceInfo(i)
	defer q.runDeferred()

	rows, err := q.ex.QueryContext(ctx, q.query.String(), q.params...)
	if err != nil {
		return q.returnErr(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return q.returnErr(err)
	}

	var fields []interface{}
	var valueMap ValueMap
	for rows.Next() {
		element := reflect.New(elemType).Elem()
		valueMap = makeValueMap(element, valueMap)
		fields = valueMap.MapToColumns(columns, fields)
		err = rows.Scan(fields...)
		if err != nil {
			return q.returnErr(err)
		}
		if elemIsPtr {
			element = element.Addr()
		}
		v.Set(reflect.Append(v, element))
		fields = fields[:0]
	}

	return nil
}

func (q *Q) Find(i interface{}) error {
	return q.FindContext(context.Background(), i)
}

func (q *Q) ScanContext(ctx context.Context, dest ...interface{}) error {
	if q.query.Len() == 0 {
		panic(errEmptyQuery)
	}
	defer q.runDeferred()

	rows, err := q.ex.QueryContext(ctx, q.query.String(), q.params...)
	if err != nil {
		return q.returnErr(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return q.returnErr(ErrNoRecord)
	}
	err = rows.Scan(dest...)
	return q.returnErr(err)
}

func (q *Q) Scan(dest ...interface{}) error {
	return q.ScanContext(context.Background(), dest...)
}

func (q *Q) ForEachContext(ctx context.Context, fn ScanFunc) error {
	if q.query.Len() == 0 {
		panic(errEmptyQuery)
	}
	defer q.runDeferred()

	rows, err := q.ex.QueryContext(ctx, q.query.String(), q.params...)
	if err != nil {
		return q.returnErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		if err = fn(q, rows); err != nil {
			return q.returnErr(err)
		}
	}

	return nil
}

func (q *Q) ForEach(fn ScanFunc) error {
	return q.ForEachContext(context.Background(), fn)
}

func (q *Q) RowsAffected() int64 {
	return q.rowsAffected
}

func (q *Q) LastInsertID() int64 {
	return q.lastInsertID
}

func (q *Q) Error() error {
	return q.err
}

func (q *Q) New() *Q {
	return New(q.ex, q.d)
}

func (q *Q) Clone() *Q {
	clone := new(Q)
	*clone = *q
	return clone
}

func (q *Q) Reset() *Q {
	q.query.Reset()
	if q.params != nil {
		q.params = q.params[:0]
	}
	q.sep = Space
	q.err = nil
	q.lastInsertID, q.rowsAffected = 0, 0
	if q.deferred != nil {
		q.deferred = q.deferred[:0]
	}
	return q
}

// Fields does the same thing as Fields() but it also sets the Dialect.
func (q *Q) Fields(i interface{}) *FieldSelector {
	return Fields(i).SetDialect(q.d)
}

func (q *Q) writeSep() {
	if q.query.Len() > 0 {
		q.query.WriteString(q.sep)
	}
}

func (q *Q) returnErr(err error) error {
	q.err = err
	return err
}

func (q *Q) runDeferred() {
	for _, fn := range q.deferred {
		fn(q)
	}
}

const (
	phName     = "{name}"
	phDataType = "{dataType}"
	phBindVar  = "{bindVar}"
)

func (q *Q) writeFormat(format, sep string, fields []Field, n int) {
	if n < 1 {
		return
	}

	var (
		hasName     = strings.Contains(format, phName)
		hasDataType = strings.Contains(format, phDataType)
		hasBindVar  = strings.Contains(format, phBindVar)
	)

	if fields == nil && (hasName || hasDataType) {
		panic("format contains placeholder {name} or {dataType}, this is not allowed when only formatting values")
	}

	fmtr := func(i int, f *Field) {
		part := format
		if hasName {
			part = strings.Replace(part, phName, f.Name, -1)
		}
		if hasDataType {
			part = strings.Replace(part, phDataType, f.DataType, -1)
		}
		if hasBindVar {
			part = strings.Replace(part, phBindVar, q.d.BindVar(q, i), -1)
		}
		q.query.WriteString(part)
	}

	fmtr(0, &fields[0])
	for i := 1; i < n; i++ {
		q.query.WriteString(sep)
		fmtr(i, &fields[i])
	}
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
