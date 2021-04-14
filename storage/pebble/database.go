package pebble

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// Database is an in-memory database.
type Database struct {
	name string
	// store a storage instance
	storage *storage
}

// $ is used here to signal whether something to be hidden/reserved
// $system.databases.<name>

// first query to support : Create/List Database
// Test Plan:
// CREATE DATABASE foo;
// should work
// do it another time, then we should get error for db already exists

// Second query: Drop Database

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	s, err := NewStorage(name)
	if err != nil {
		return nil
	}
	return &Database{
		name:    name,
		storage: s,
	}
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Tables returns all tables in the database.
// Show tables query probably uses this
// naming convention for tables: system.databases.<foo>.tables.<bar> or <foo>.tables.<bar>
func (d *Database) Tables() map[string]sql.Table {
	//TODO: figure out how to list all keys contain `table`
	return nil
}

func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	return nil, false, nil

}

// GetTableNames returns the table names of every table in the database
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	return nil, nil
}

// AddTable adds a new table to the database.
func (d *Database) AddTable(name string, t sql.Table) error {
	key := "system.databases." + d.name + ".tables." + name
	return d.storage.create(key)
}

// Create creates a table with the given name and schema
func (d *Database) Create(name string, schema sql.Schema) error {
	// "system" is the prefix(naming convention for identifying a database key)
	key := "system.databases." + name
	return d.storage.create(key)
}

// List returns all databases stored.
func (d *Database) List() ([]string, error) {
	// TODO: how to list?
	return nil, nil
}
