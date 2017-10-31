package mysql

import (
	"reflect"

	"github.com/semrekkers/sugar"

	go_mysql "github.com/go-sql-driver/mysql"
)

var (
	reflectTypeNullTime = reflect.TypeOf(go_mysql.NullTime{})
)

func Open(dataSourceName string) (*sugar.DB, error) {
	return sugar.OpenSpecial("mysql", dataSourceName, sugar.DefaultBindVar, TypeMapper)
}

func TypeMapper(t reflect.Type) (out string, ok bool) {
	out, ok = sugar.DefaultTypeMapper(t)
	if !ok {
		switch t {
		case reflectTypeNullTime:
			ok, out = true, "DATETIME NULL"
		}
	}
	return
}

type DBInfo struct{}

func (DBInfo) HasTable(db *sugar.DB, tableName string) (tableExists bool, err error) {
	err = db.Querier().
		Write("SELECT EXISTS ( SELECT table_name FROM information_schema.tables WHERE table_schema = (SELECT DATABASE())").
		Write("AND table_name = ? )", tableName).
		Scan(&tableExists)

	return
}

func (DBInfo) TableColumns(db *sugar.DB, tableName string) (columns []string, err error) {
	err = db.Querier().
		Write("SELECT column_name FROM information_schema.columns WHERE table_schema = (SELECT DATABASE())").
		Write("AND table_name = ?", tableName).
		ForEach(sugar.AppendToStringSlice(&columns))

	return
}
