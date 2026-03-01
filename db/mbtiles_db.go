package db

import (
	"context"
	"database/sql"
	"log"

	"github.com/duckdb/duckdb-go/v2"
)

func OpenMBTilesAppender() *duckdb.Appender {
	db, err := sql.Open("duckdb", "tmp/out.db")
	if err != nil {
		log.Fatalln(err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS tiles (
			z INT,
			x INT,
			y INT,
			tile_data BLOB
		);

		-- empty the tiles database
		DELETE FROM tiles;
	`); err != nil {
		log.Fatalln(err)
	}

	db.Close()

	connector, err := duckdb.NewConnector("tmp/out.db", nil)
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	appender, err := duckdb.NewAppenderFromConn(conn, "", "tiles")
	if err != nil {
		log.Fatalln(err)
	}

	return appender
}