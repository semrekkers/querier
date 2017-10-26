package migrator

import "github.com/semrekkers/sugar"

const (
	Initialize = ""
)

type Model interface {
	TableName() string
	Migrate(*sugar.Querier, string)
}

type DBInfo interface {
	HasTable(sugar.Executor, string) (bool, error)
	TableColumns(sugar.Executor, string) ([]string, error)
}

type Migrator struct {
	ex      sugar.Executor
	bindVar sugar.Formatter
	mapper  sugar.TypeMapper
	dbInfo  DBInfo
}

type Result struct {
	Tables, Columns []string
}

func New(ex sugar.Executor, bindVar sugar.Formatter, mapper sugar.TypeMapper, dbInfo DBInfo) *Migrator {
	return &Migrator{ex, bindVar, mapper, dbInfo}
}

func NewFromDB(db *sugar.DB, dbInfo DBInfo) *Migrator {
	return &Migrator{db.DB, db.BindVar, db.Mapper, dbInfo}
}

func (m *Migrator) Migrate(models ...Model) (*Result, error) {
	var result Result

	for _, model := range models {
		tableName := model.TableName()

		tableExists, newColumns, err := m.migrateModel(tableName, model)
		if err != nil {
			return nil, err
		}
		if tableExists {
			result.Tables = append(result.Tables, tableName)
		} else {
			result.Columns = append(result.Columns, newColumns...)
		}
	}

	return &result, nil
}

func (m *Migrator) migrateModel(tableName string, model Model) (tableExists bool, newColumns []string, err error) {
	tableExists, err = m.dbInfo.HasTable(m.ex, tableName)
	if err != nil {
		return
	}

	q := sugar.NewQuerier(m.ex, sugar.DefaultBindVar)
	fieldSelector := sugar.Fields(model).SetTypeMapper(m.mapper)
	if !tableExists {
		q.Writef("CREATE TABLE %s (", tableName)
		q.FieldDefinitions("", fieldSelector.Select())
		model.Migrate(q, Initialize)
		q.WriteString(")")
	} else {
		var columns []string
		columns, err = m.dbInfo.TableColumns(m.ex, tableName)
		if err != nil {
			return
		}
		fields := fieldSelector.Except(columns...).Select()
		if len(fields) == 0 {
			return
		}

		q.Writef("ALTER TABLE %s (", tableName)
		q.FieldDefinitions("ADD ", fields)
		for _, field := range fields {
			model.Migrate(q, field.Name)
			newColumns = append(newColumns, tableName+"."+field.Name)
		}
		q.WriteString(")")
	}
	err = q.Exec()

	return
}
