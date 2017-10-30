package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/semrekkers/sugar/migrator"

	"github.com/semrekkers/sugar"

	go_mysql "github.com/go-sql-driver/mysql"
)

var (
	address  = os.Getenv("SUGAR_ADDRESS")
	username = os.Getenv("SUGAR_USERNAME")
	password = os.Getenv("SUGAR_PASSWORD")
	database = os.Getenv("SUGAR_DATABASE")

	conn *sql.Conn
)

type dummy struct {
	ID        uint `db:",BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"`
	Name      string
	FirstName string
	LastName  string
	Age       sql.NullInt64
	CreatedAt time.Time
	DeletedAt go_mysql.NullTime
}

func (*dummy) TableName() string {
	return "dummy_table"
}

func (*dummy) Migrate(q *sugar.Querier, column string) {
	switch column {
	case migrator.Initialize:
		q.Append("PRIMARY KEY (ID)")
		q.Append("UNIQUE (Name)")
	}
}

func TestMain(m *testing.M) {
	dsn := fmt.Sprintf("%s:%s@%s/", username, password, address)
	db, err := sugar.Open("mysql", dsn)
	exitOnErr("open database", err)
	defer db.Close()
	conn, err = db.Conn(context.Background())
	exitOnErr("open connection", err)
	defer conn.Close()

	q := sugar.NewQuerier(conn, sugar.DefaultBindVar)
	err = q.Writef("CREATE DATABASE IF NOT EXISTS %s", database).Defer(resetQ).Exec()
	exitOnErr("create test database", err)
	err = q.Writef("USE %s", database).Defer(resetQ).Exec()
	exitOnErr("use test database", err)

	os.Exit(m.Run())
}

func TestMigration(t *testing.T) {
	var model dummy
	m := migrator.New(conn, sugar.DefaultBindVar, TypeMapper, DBInfo{})
	res, err := m.Migrate(&model)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("migration result:", res)

	err = sugar.NewQuerier(conn, sugar.DefaultBindVar).
		Writef("DROP TABLE %s", model.TableName()).
		Exec()
	if err != nil {
		t.Error("drop dummy table:", err)
	}
}

func exitOnErr(name string, err error) {
	if err != nil {
		fmt.Printf("error: %s: %s\n", name, err.Error())
		os.Exit(1)
	}
}

func resetQ(q *sugar.Querier) {
	q.Reset()
}
