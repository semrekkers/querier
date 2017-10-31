// Package migrator implements a simple Model migrator using Sugar.
package migrator

import (
	"fmt"

	"github.com/semrekkers/sugar"
)

// Model that can be created or migrated. It contains callbacks that are called when migrating.
type Model interface {
	// TableName returns the model's table name.
	TableName() string

	// CreateTable is called before the table is created. This is useful for, e.g. defining the primary key.
	CreateTable(*sugar.Querier) error

	// Migrate is called when the migrator discovered a new field in the model.
	Migrate(db *sugar.DB, column string) error
}

// DBInfo is an interface for retrieving information about the database.
type DBInfo interface {
	HasTable(*sugar.DB, string) (bool, error)
	TableColumns(*sugar.DB, string) ([]string, error)
}

// Migrator is the actual migrator. It is safe for multiple goroutines to call it's methods.
type Migrator struct {
	db     *sugar.DB
	dbInfo DBInfo
}

// Result contains the results of a successful migration.
type Result struct {
	TablesCreated, NewColumns []string
}

// MigrationError describes a problem encountered during the migration.
type MigrationError struct {
	Table, Column string
	Err           error
}

func (e *MigrationError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("migration table %s, column %s: %s", e.Table, e.Column, e.Err.Error())
	}
	return fmt.Sprintf("migration table %s: %s", e.Table, e.Err.Error())
}

// New returns a new Migrator.
func New(db *sugar.DB, dbInfo DBInfo) *Migrator {
	return &Migrator{db, dbInfo}
}

// Migrate migrates the models.
func (m *Migrator) Migrate(models ...Model) (*Result, error) {
	var res Result

	for _, model := range models {
		if err := m.migrateModel(model, &res); err != nil {
			return nil, err
		}
	}

	return &res, nil
}

// Drop drops the models.
func (m *Migrator) Drop(models ...Model) error {
	for _, model := range models {
		tableName := model.TableName()
		err := m.db.Querier().Writef("DROP TABLE %s", tableName).Exec()
		if err != nil {
			return &MigrationError{Table: tableName, Err: err}
		}
	}
	return nil
}

func (m *Migrator) migrateModel(model Model, res *Result) error {
	tableName := model.TableName()
	tableExists, err := m.dbInfo.HasTable(m.db, tableName)
	if err != nil {
		return err
	}

	fieldSelector := m.db.Fields(model)
	if !tableExists {
		q := m.db.Querier().Writef("CREATE TABLE %s (", tableName).
			WriteFields("{name} {dataType}", sugar.FieldSep, fieldSelector.Select()...)
		q.SetSeparator(sugar.FieldSep)
		if err = model.CreateTable(q); err != nil {
			return &MigrationError{Table: tableName, Err: err}
		}
		q.WriteRaw(")")
		if err = q.Exec(); err != nil {
			return &MigrationError{Table: tableName, Err: err}
		}
		res.TablesCreated = append(res.TablesCreated, tableName)
	} else {
		existing, err := m.dbInfo.TableColumns(m.db, tableName)
		if err != nil {
			return err
		}

		for _, field := range fieldSelector.Except(existing...).Select() {
			err = m.db.Querier().Writef("ALTER TABLE %s", tableName).
				WriteFields("ADD {name} {dataType}", "", field).
				Exec()
			if err != nil {
				return &MigrationError{Table: tableName, Column: field.Name, Err: err}
			}
			if err = model.Migrate(m.db, field.Name); err != nil {
				return &MigrationError{Table: tableName, Column: field.Name, Err: err}
			}
			res.NewColumns = append(res.NewColumns, tableName+"."+field.Name)
		}
	}

	return nil
}
