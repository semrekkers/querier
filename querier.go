package sugar

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
type DeferFunc func(*Querier)

// ScanFunc is called for each row in the result set.
type ScanFunc func(*Querier, *sql.Rows) error

// Formatter formats a query or a part of a query.
type Formatter func(*Querier, int) string

// Querier can build and execute queries.
type Querier struct {
	ex Executor

	// Formatters
	bindVar Formatter

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

func NewQuerier(ex Executor, bindVar Formatter) *Querier {
	return &Querier{ex: ex, bindVar: bindVar, sep: Space}
}

// Write writes a string (query) to the Querier. A single space is appended after query.
func (q *Querier) Write(query string, params ...interface{}) *Querier {
	q.writeSep()
	q.query.WriteString(query)
	q.params = append(q.params, params...)
	return q
}

// Writef writes a formatted string (format) to the Querier. A single space is appended after query.
func (q *Querier) Writef(format string, args ...interface{}) *Querier {
	q.writeSep()
	q.query.WriteString(fmt.Sprintf(format, args...))
	return q
}

func (q *Querier) WriteFields(format, sep string, fields ...Field) *Querier {
	q.writeSep()
	q.writeFormat(format, sep, fields, len(fields))
	return q
}

func (q *Querier) WriteValues(format, sep string, values ...interface{}) *Querier {
	q.writeSep()
	q.writeFormat(format, sep, nil, len(values))
	q.params = append(q.params, values...)
	return q
}

func (q *Querier) WriteValueMap(format, sep string, valueMap ValueMap, fields ...Field) *Querier {
	q.writeSep()
	q.writeFormat(format, sep, fields, len(fields))
	q.params = append(q.params, valueMap.MapToFields(fields, nil)...)
	return q
}

func (q *Querier) WriteRaw(s string) *Querier {
	q.query.WriteString(s)
	return q
}

func (q *Querier) PreWrite() *Querier {
	if q.preWrite != "" {
		q.writeSep()
		q.query.WriteString(q.preWrite)
	}
	return q
}

func (q *Querier) SetPreWrite(s string) *Querier {
	q.preWrite = s
	return q
}

func (q *Querier) SetSeparator(sep string) *Querier {
	q.sep = sep
	return q
}

func (q *Querier) AddParams(params ...interface{}) *Querier {
	q.params = append(q.params, params...)
	return q
}

func (q *Querier) Params() []interface{} {
	return q.params
}

func (q *Querier) String() string {
	return q.query.String()
}

func (q *Querier) Defer(fn DeferFunc) *Querier {
	q.deferred = append(q.deferred, fn)
	return q
}

func (q *Querier) DeferSuccess(fn DeferFunc) *Querier {
	q.deferred = append(q.deferred, func(q *Querier) {
		if q.err == nil {
			fn(q)
		}
	})
	return q
}

func (q *Querier) ExecContext(ctx context.Context) error {
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

func (q *Querier) Exec() error {
	return q.ExecContext(context.Background())
}

func (q *Querier) FirstContext(ctx context.Context, i interface{}) error {
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
	err = rows.Scan(valueMap.MapToColumns(columns, nil))
	return q.returnErr(err)
}

func (q *Querier) First(i interface{}) error {
	return q.FirstContext(context.Background(), i)
}

func (q *Querier) FindContext(ctx context.Context, i interface{}) error {
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

func (q *Querier) Find(i interface{}) error {
	return q.FindContext(context.Background(), i)
}

func (q *Querier) ScanContext(ctx context.Context, dest ...interface{}) error {
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

func (q *Querier) Scan(dest ...interface{}) error {
	return q.ScanContext(context.Background(), dest...)
}

func (q *Querier) ForEachContext(ctx context.Context, fn ScanFunc) error {
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

func (q *Querier) ForEach(fn ScanFunc) error {
	return q.ForEachContext(context.Background(), fn)
}

func (q *Querier) RowsAffected() int64 {
	return q.rowsAffected
}

func (q *Querier) LastInsertID() int64 {
	return q.lastInsertID
}

func (q *Querier) Error() error {
	return q.err
}

func (q *Querier) New() *Querier {
	return NewQuerier(q.ex, q.bindVar)
}

func (q *Querier) Reset() *Querier {
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

func (q *Querier) writeSep() {
	if q.query.Len() > 0 {
		q.query.WriteString(q.sep)
	}
}

func (q *Querier) returnErr(err error) error {
	q.err = err
	return err
}

func (q *Querier) runDeferred() {
	for _, fn := range q.deferred {
		fn(q)
	}
}

const (
	phName     = "{name}"
	phDataType = "{dataType}"
	phBindVar  = "{bindVar}"
)

func (q *Querier) writeFormat(format, sep string, fields []Field, n int) {
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
			part = strings.Replace(part, phBindVar, q.bindVar(q, i), -1)
		}
		q.query.WriteString(part)
	}

	fmtr(0, &fields[0])
	for i := 1; i < n; i++ {
		q.query.WriteString(sep)
		fmtr(i, &fields[i])
	}
}
