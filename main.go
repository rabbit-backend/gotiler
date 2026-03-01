package main

import (
	"context"
	"log"

	"github.com/rabbit-backend/gotiler/db"
	"github.com/rabbit-backend/gotiler/queries"
)

func main() {
	minZoom := 0
	maxZoom := 20

	conn := db.OpenDB()

	var totalFeatures int
	conn.QueryRowContext(context.TODO(), queries.TOTAL_FEATURES).Scan(&totalFeatures)

	log.Println("Total Features:", totalFeatures)

	for i := minZoom; i < maxZoom; i++ {
		log.Println("Generating Tiles for Zoom", i)
		if _, err := conn.ExecContext(
			context.Background(), 
			queries.GENERATE_TILE_COVERS_FOR_ZOOM, 
			i,
		); err != nil {
			log.Fatalln(err)
		}

		tolerance :=  float64(uint32(1) << i) / float64(totalFeatures)
		if tolerance > 1 {
			tolerance = 1
		}

		if i >= 14 {
			tolerance = 0
		}

		rows, err := conn.QueryContext(
			context.Background(), 
			queries.GENERATE_MVT_GEOM, 
			tolerance, 
			i,
		)
		if err != nil {
			log.Fatalln(err)
		}

		for rows.Next() {
			var z, x, y uint64
			var mvt []byte

			rows.Scan(&z, &x, &y, &mvt)
			log.Println(z, x, y, string(mvt))
		}
	}
}