package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/maptile/tilecover"
)

func main() {
	MIN_ZOOM := 10
	MAX_ZOOM := 22

	var lock sync.RWMutex
	var wg sync.WaitGroup

	t1 := time.Now()

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
		for zoom := MIN_ZOOM; zoom <= MAX_ZOOM; zoom++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				tiles, err := tilecover.Geometry(s.Geometry, maptile.Zoom(zoom))
				if err != nil {
					wg.Done()
					return
				}

				for range tiles {
					lock.Lock()
					count += 1
					lock.Unlock()
				}

				wg.Done()
			}(&wg)

		}
	}

	wg.Wait()

	log.Println("[x] total records:", count)
	t2 := time.Now()
	log.Println("[x] processed:", count)

	fmt.Println("[x] total time:", t2.Unix()-t1.Unix())
}
