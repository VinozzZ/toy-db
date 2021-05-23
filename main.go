package main

import (
	"time"

	"github.com/VinozzZ/toy-db/db"
	"github.com/VinozzZ/toy-db/db/storage"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

// Example of how to implement a MySQL server based on a Engine:
//
// ```
// > mysql --host=127.0.0.1 --port=5123 -u user -ppass db -e "SELECT * FROM mytable"
// +----------+-------------------+-------------------------------+---------------------+
// | name     | email             | phone_numbers                 | created_at          |
// +----------+-------------------+-------------------------------+---------------------+
// | John Doe | john@doe.com      | ["555-555-555"]               | 2018-04-18 09:41:13 |
// | John Doe | johnalt@doe.com   | []                            | 2018-04-18 09:41:13 |
// | Jane Doe | jane@doe.com      | []                            | 2018-04-18 09:41:13 |
// | Evil Bob | evilbob@gmail.com | ["555-666-555","666-666-666"] | 2018-04-18 09:41:13 |
// +----------+-------------------+-------------------------------+---------------------+
// ```
func main() {
	s, err := storage.NewDefaultStorage()
	if err != nil {
		panic(err)
	}
	engine := sqle.NewDefault()
	// TODO: load up existing databases, put it into catalog
	// CREATE database
	// find the function being called when server receives `CREATE DATABASE`
	engine.AddDatabase(createPebbleDatabase(s))

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("root", "", auth.AllPermissions),
	}

	mysqlServer, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}

	mysqlServer.Start()
}

func createPebbleDatabase(s *storage.Store) *db.Database {
	return db.NewDatabase("", s)
}

func createTestDatabase() *memory.Database {
	const (
		dbName    = "mydb"
		tableName = "mytable"
	)

	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()
	table.Insert(ctx, sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()))
	table.Insert(ctx, sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()))
	table.Insert(ctx, sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()))
	return db
}
