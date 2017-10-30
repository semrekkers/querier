package sugar

import "database/sql"

// DB wraps sql.DB with some extra functionalities. See database/sql.DB for more information.
type DB struct {
	*sql.DB

	// BindVar is the default bindvar formatter for this database.
	BindVar Formatter
	// Mapper is the default type mapper for this database.
	Mapper TypeMapper
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

// Fields returns a field selector with the database's TypeMapper. See sugar.Fields for more information.
func (db *DB) Fields(i interface{}) *FieldSelector {
	return Fields(i).SetTypeMapper(db.Mapper)
}

// Querier returns a new Querier for this database.
func (db *DB) Querier() *Querier {
	return &Querier{
		ex:      db.DB,
		bindVar: db.BindVar,
	}
}
