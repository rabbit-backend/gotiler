package db

import (
	"database/sql"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rabbit-backend/gotiler/queries"
)

func OpenDB() *sql.DB {
	// open the persistent duckdb connection
	db, err := sql.Open("duckdb", "tmp/features.db")
	if err != nil {
		log.Fatalln(err)
	}

	// initialization script for the db
	if _, err := db.Exec(queries.INITIALIZE_DB); err != nil {
		log.Fatalln(err)
	}

	return db
}