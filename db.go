package sugar

import "database/sql"

// DB wraps sql.DB with some extra functionalities. See database/sql.DB for more information.
type DB struct {
	*sql.DB

	bindVar Formatter
	mapper  TypeMapper
}

// Open opens and pings a database. See database/sql.Open for more information.
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

func OpenSpecial(driverName, dataSourceName string, bindVar Formatter, mapper TypeMapper) (*DB, error) {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	db.bindVar = bindVar
	db.mapper = mapper
	return db, nil
}

// Fields returns a field selector with the database's TypeMapper. See sugar.Fields for more information.
func (db *DB) Fields(i interface{}) *FieldSelector {
	return Fields(i).SetTypeMapper(db.mapper)
}

// Querier returns a new Querier for this database.
func (db *DB) Querier() *Querier {
	return NewQuerier(db.DB, db.bindVar)
}
