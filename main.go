package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/maptile"
)

func main() {
	MIN_ZOOM := 10
	MAX_ZOOM := 22

	reader, err := sql.Open("duckdb", "db/spatial.db")
	if err != nil {
		log.Fatalln(err)
	}

	defer reader.Close()

	if _, err := reader.Exec(`INSTALL spatial; LOAD spatial;`); err != nil {
		log.Fatalln(err)
	}

	writer, err := sql.Open("duckdb", "db/out.db")
	if err != nil {
		log.Fatalln(err)
	}

	defer writer.Close()

	if _, err := writer.Exec(`INSTALL spatial; LOAD spatial;`); err != nil {
		log.Fatalln(err)
	}

	if _, err := writer.Exec(`
		CREATE TABLE IF NOT EXISTS temp (
			z INTEGER,
			x INTEGER,
			y INTEGER,
			features BLOB
		);

		DELETE FROM temp;
		`,
	); err != nil {
		log.Fatalln(err)
	}

	connector, err := duckdb.NewConnector("db/out.db", nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "temp")
	log.Println(appender)

	log.Println("[x] started generating the vector tiles...")

	rows, err := reader.QueryContext(context.Background(), `SELECT id, ST_AsWKB(geom) FROM polygons`)
	if err != nil {
		log.Fatalln(err)
	}

	count := 0
	for rows.Next() {
		var id int
		s := wkb.Scanner(nil)

		rows.Scan(&id, &s)
		bounds := s.Geometry.Bound()

		for zoom := MIN_ZOOM; zoom <= MAX_ZOOM; zoom++ {
			tmin := maptile.At(bounds.Min, maptile.Zoom(zoom))
			tmax := maptile.At(bounds.Max, maptile.Zoom(zoom))

			for x := tmin.X; x <= tmax.X; x += 1 {
				for y := tmin.Y; y <= tmax.Y; y += 1 {
					count += 1
				}
			}
		}

		if count%100_000 == 0 {
			log.Println("[x] processed:", count)
		}
	}

	log.Println("[x] processed:", count)
}
