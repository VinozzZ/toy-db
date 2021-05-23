package db

import (
	"github.com/VinozzZ/toy-db/db/storage"
	"github.com/dolthub/go-mysql-server/sql"
)

// pros vs cons when using a single or separate pebbel storage engine
// 1. lay out on different harddrives
// when deleting a db, we can just delete a file instead of all the rows of records
// 2. loose transaction property across databases
// have to build transaction guarantees layer in our own application layer
// could be a feature to allow user to specify whether to use shared storage engine or not

// distributed database
// 1. network level transaction
// 2. file system level
// block storage

// Database is an in-memory database.
type Database struct {
	name string
	// store a storage instance
	store *storage.Store
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)

// $ is used here to signal whether something to be hidden/reserved
// $system.databases.<name>

// first query to support : Create/List Database
// Test Plan:
// CREATE DATABASE foo;
// should work
// do it another time, then we should get error for db already exists

// Second query: Drop Database

// NewDatabase creates a new database with the given name.
func NewDatabase(name string, s *storage.Store) *Database {
	return &Database{
		name:  name,
		store: s,
	}
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	return nil, false, nil
}

// GetTableNames returns the table names of every table in the database
// Show tables query probably uses this
// naming convention for tables: system.databases.<foo>.tables.<bar> or <foo>.tables.<bar>
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	return nil, nil
}

// Create creates a table with the given name and schema
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	// "system" is the prefix(naming convention for identifying a database key)
	key := "system.table." + name
	return d.store.Create(key)
}
