package mysql

import (
	"reflect"

	"github.com/semrekkers/querier"

	go_mysql "github.com/go-sql-driver/mysql"
)

var (
	reflectTypeNullTime = reflect.TypeOf(go_mysql.NullTime{})
)

type Dialect struct {
	querier.Dialect
}

func (d Dialect) TypeMapper(t reflect.Type) (dataType string, ok bool) {
	dataType, ok = d.TypeMapper(t)
	if !ok && t == reflectTypeNullTime {
		return "DATETIME NULL", true
	}
	return
}

func (Dialect) HasTable(q *querier.Q, tableName string) (tableExists bool, err error) {
	err = q.
		Write("SELECT EXISTS ( SELECT table_name FROM information_schema.tables WHERE table_schema = (SELECT DATABASE())").
		Write("AND table_name = ? )", tableName).
		Scan(&tableExists)

	return
}

func (Dialect) TableColumns(q *querier.Q, tableName string) (columns []string, err error) {
	err = q.
		Write("SELECT column_name FROM information_schema.columns WHERE table_schema = (SELECT DATABASE())").
		Write("AND table_name = ?", tableName).
		ForEach(querier.AppendToStringSlice(&columns))

	return
}
