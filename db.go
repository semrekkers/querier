package sugar

import "database/sql"

type DB struct {
	*sql.DB

	BindVar Formatter
	Mapper  TypeMapper
}

func Open(driverName, dataSourceName string) (*DB, error) {
	handle, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = handle.Ping(); err != nil {
		return nil, err
	}
	return &DB{
		handle,
		DefaultBindVar,
		DefaultTypeMapper,
	}, nil
}

func (db *DB) Fields(i interface{}) *FieldSelector {
	return Fields(i).SetTypeMapper(db.Mapper)
}

func (db *DB) Querier() *Querier {
	return &Querier{
		ex:      db.DB,
		bindVar: db.BindVar,
	}
}
