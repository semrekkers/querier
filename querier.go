package sugar

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

const (
	fieldSep = ", "
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
type Formatter func(*Querier, int)

// Querier can build and execute queries.
type Querier struct {
	ex Executor

	// Formatters and mappers
	bindVar Formatter

	// Query builder
	query         bytes.Buffer
	params        []interface{}
	insertSepNext bool

	// For deffered functions.
	err          error
	lastInsertID int64
	rowsAffected int64
	deferred     []DeferFunc
}

func NewQuerier(ex Executor, bindVar Formatter) *Querier {
	return &Querier{ex: ex, bindVar: bindVar}
}

// Write writes a string (query) to the Querier. A single space is appended after query.
func (q *Querier) Write(query string, params ...interface{}) *Querier {
	q.insertSep()
	q.writeSpace(query)
	q.params = append(q.params, params...)
	return q
}

// Writef writes a formatted string (format) to the Querier. A single space is appended after query.
func (q *Querier) Writef(format string, args ...interface{}) *Querier {
	q.insertSep()
	q.writeSpace(fmt.Sprintf(format, args...))
	return q
}

// Append appends a string (query) to the Querier. An field separator is also appended if necessary.
func (q *Querier) Append(query string, params ...interface{}) *Querier {
	q.insertSep()
	q.query.WriteString(query)
	q.params = append(q.params, params...)
	q.insertSepNext = true
	return q
}

// Appendf appends a formatted string (format) to the Querier. An field separator is also appended if necessary.
func (q *Querier) Appendf(format string, args ...interface{}) *Querier {
	q.insertSep()
	q.query.WriteString(fmt.Sprintf(format, args...))
	q.insertSepNext = true
	return q
}

func (q *Querier) AppendParams(params ...interface{}) *Querier {
	q.params = append(q.params, params...)
	return q
}

func (q *Querier) FieldDefinitions(prefix string, fields []Field) *Querier {
	q.insertSep()
	q.writeFields(prefix, fields, false, true, false)
	q.insertSepNext = true
	return q
}

func (q *Querier) Fields(array bool, fields []Field) *Querier {
	q.insertSep()
	q.writeFields("", fields, array, false, false)
	return q
}

func (q *Querier) Values(array bool, values ...interface{}) *Querier {
	q.insertSep()
	q.writeBindVars(len(values), array)
	q.params = append(q.params, values...)
	return q
}

func (q *Querier) ValueMap(array bool, fields []Field, values ValueMap) *Querier {
	q.insertSep()
	q.writeBindVars(len(fields), array)
	q.params = append(q.params, values.MapToFields(fields, nil)...)
	return q
}

func (q *Querier) SetValues(fields []Field, values ValueMap) *Querier {
	q.insertSep()
	q.writeFields("", fields, false, false, true)
	q.params = append(q.params, values.MapToFields(fields, nil)...)
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

func (q *Querier) WriteString(s string) {
	q.query.WriteString(s)
}

func (q *Querier) New() *Querier {
	return &Querier{ex: q.ex, bindVar: q.bindVar}
}

func (q *Querier) Reset() *Querier {
	q.query.Reset()
	if q.params != nil {
		q.params = q.params[:0]
	}
	q.insertSepNext = false
	q.err = nil
	q.lastInsertID, q.rowsAffected = 0, 0
	if q.deferred != nil {
		q.deferred = q.deferred[:0]
	}
	return q
}

func (q *Querier) insertSep() {
	if q.insertSepNext {
		q.query.WriteString(fieldSep)
		q.insertSepNext = false
	}
}

func (q *Querier) writeSpace(s string) {
	q.query.WriteString(s)
	q.query.WriteString(" ")
}

func (q *Querier) writeFields(prefix string, fields []Field, array, defs, sets bool) {
	if array {
		q.query.WriteString("(")
	}

	if len(fields) != 0 {
		q.query.WriteString(prefix)
		q.query.WriteString(fields[0].Name)
		if defs {
			q.query.WriteString(" ")
			q.query.WriteString(fields[0].DataType)
		}
		if sets {
			q.query.WriteString(" = ")
			q.bindVar(q, 0)
		}
		for i := 1; i < len(fields); i++ {
			q.query.WriteString(fieldSep)
			q.query.WriteString(prefix)
			q.query.WriteString(fields[i].Name)
			if defs {
				q.query.WriteString(" ")
				q.query.WriteString(fields[i].DataType)
			}
			if sets {
				q.query.WriteString(" = ")
				q.bindVar(q, i)
			}
		}
	}

	if array {
		q.query.WriteString(")")
	}
}

func (q *Querier) writeBindVars(n int, array bool) {
	if array {
		q.query.WriteString("(")
	}

	if n > 0 {
		q.bindVar(q, 0)
		for i := 1; i < n; i++ {
			q.query.WriteString(fieldSep)
			q.bindVar(q, i)
		}
	}

	if array {
		q.query.WriteString(")")
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
