package db

import (
	"context"
	"database/sql"
	"log"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rabbit-backend/gotiler/queries"
)

func OpenDB() *sql.Conn {
	// open the persistent duckdb connection
	db, err := sql.Open("duckdb", "tmp/features.db")
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatalln(conn)
	}

	// register the scalar udf to generate tile cover
	duckdb.RegisterScalarUDF(conn, "ST_TileCover", &TileCoverUDF{})

	// initialization script for the db
	if _, err := conn.ExecContext(context.TODO(), queries.INITIALIZE_DB); err != nil {
		log.Fatalln(err)
	}

	return conn
}