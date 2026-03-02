package main

import (
	"bytes"
	"context"
	"log"

	"github.com/paulmach/orb/encoding/mvt"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/maptile"
	"github.com/rabbit-backend/gotiler/db"
	"github.com/rabbit-backend/gotiler/queries"
)

func main() {
	minZoom := 0
	maxZoom := 14

	conn := db.OpenDB()
	appender := db.OpenMBTilesAppender()

	var totalFeatures int
	conn.QueryRowContext(context.TODO(), queries.TOTAL_FEATURES).Scan(&totalFeatures)

	log.Println("Total Features:", totalFeatures)

	for i := minZoom; i <= maxZoom; i++ {
		log.Println("Generating Tiles for Zoom", i)
		if _, err := conn.ExecContext(
			context.Background(), 
			queries.GENERATE_TILE_COVERS_FOR_ZOOM, 
			i,
		); err != nil {
			log.Fatalln(err)
		}

		tolerance :=  1 / (float64(uint32(1) << i))
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

		featuresMap := make(map[uint64](*geojson.FeatureCollection))

		for rows.Next() {
			var z, x, y uint64
			var geomWKB []byte

			rows.Scan(&z, &x, &y, &geomWKB)
			geom ,err := wkb.NewDecoder(bytes.NewBuffer(geomWKB)).Decode()
			if err != nil {
				log.Fatalln(err)
			}

			tileID := db.ZxyToID(uint8(z), uint32(x), uint32(y))
			
			_, ok := featuresMap[tileID]
			if !ok {
				featuresMap[tileID] = geojson.NewFeatureCollection()
				featuresMap[tileID].Append(geojson.NewFeature(geom))
 			} else {
				featuresMap[tileID].Append(geojson.NewFeature(geom))
			}
		}

		for tileID := range featuresMap {
			z, x, y := db.IDToZxy(tileID)
			tile := maptile.New(x, y, maptile.Zoom(z))
			
			layers := mvt.NewLayers(map[string]*geojson.FeatureCollection{
				"data": featuresMap[tileID],
			})

			layers.ProjectToTile(tile)
			layers.Clip(mvt.MapboxGLDefaultExtentBound)

			blob, _ := mvt.Marshal(layers)
			appender.AppendRow(
				z, x, (1 << z) - 1 - y, blob,
			)
		}
	}

	appender.Flush()
}