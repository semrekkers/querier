package mysql

// import (
// 	"database/sql"
// 	"fmt"
// 	"os"
// 	"testing"
// 	"time"

// 	"github.com/semrekkers/sugar/migrator"

// 	"github.com/semrekkers/sugar"

// 	go_mysql "github.com/go-sql-driver/mysql"
// )

// var (
// 	address  = os.Getenv("SUGAR_ADDRESS")
// 	username = os.Getenv("SUGAR_USERNAME")
// 	password = os.Getenv("SUGAR_PASSWORD")
// 	database = os.Getenv("SUGAR_DATABASE")

// 	db *sugar.DB
// )

// type dummy struct {
// 	ID        uint `db:",BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"`
// 	Name      string
// 	FirstName string
// 	LastName  string
// 	Age       sql.NullInt64
// 	CreatedAt time.Time
// 	DeletedAt go_mysql.NullTime
// }

// func (*dummy) TableName() string {
// 	return "dummy_table"
// }

// func (*dummy) CreateTable(q *sugar.Querier) {
// 	q.Write("PRIMARY KEY (ID)")
// 	q.Write("UNIQUE (Name)")
// }

// func (*dummy) Migrate(db *sugar.DB, column string) error {
// 	// Nothing to migrate (yet).
// 	return nil
// }

// func TestMain(m *testing.M) {
// 	var err error

// 	// Create test database.
// 	dsn := fmt.Sprintf("%s:%s@%s/", username, password, address)
// 	db, err = sugar.Open("mysql", dsn)
// 	exitOnErr("open database", err)
// 	err = db.Querier().Writef("CREATE DATABASE IF NOT EXISTS %s", database).Exec()
// 	exitOnErr("create test database", err)
// 	db.Close()

// 	// Open connection.
// 	dsn = fmt.Sprintf("%s:%s@%s/%s", username, password, address, database)
// 	db, err = sugar.OpenSpecial("mysql", dsn, sugar.DefaultBindVar, TypeMapper)
// 	exitOnErr("open database", err)
// 	defer db.Close()

// 	// And test!
// 	os.Exit(m.Run())
// }

// func TestMigration(t *testing.T) {
// 	var model dummy
// 	m := migrator.New(db, DBInfo{})

// 	res, err := m.Migrate(&model)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Log("migration result:", res)

// 	if err = m.Drop(&model); err != nil {
// 		t.Fatal(err)
// 	}
// }

// func exitOnErr(name string, err error) {
// 	if err != nil {
// 		fmt.Printf("error: %s: %s\n", name, err.Error())
// 		os.Exit(1)
// 	}
// }
