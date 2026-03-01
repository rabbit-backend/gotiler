package db

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"log"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/maptile/tilecover"
)

type TileCoverUDF struct{}

func checkError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

type TileCover struct {
	Z uint64
	X uint64
	Y uint64

}

// Config implements [duckdb.ScalarFunc].
func (t *TileCoverUDF) Config() duckdb.ScalarFuncConfig {
	zoomInputTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	checkError(err)

	geometryTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_BLOB)
	checkError(err)


	// store the result as a z, x, y struct
	uintType, err := duckdb.NewTypeInfo(duckdb.TYPE_UINTEGER)
	checkError(err)

	zStructEntry, err := duckdb.NewStructEntry(uintType, "Z")
	checkError(err)

	xStructEntry, err := duckdb.NewStructEntry(uintType, "X")
	checkError(err)

	yStructEntry, err := duckdb.NewStructEntry(uintType, "Y")
	checkError(err)

	tileType, err := duckdb.NewStructInfo(zStructEntry, xStructEntry, yStructEntry)
	checkError(err)

	tileIDListType, err := duckdb.NewListInfo(tileType)
	checkError(err)

	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{zoomInputTypeInfo, geometryTypeInfo},
		ResultTypeInfo: tileIDListType,
	}
}

// Executor implements [duckdb.ScalarFunc].
func (t *TileCoverUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			if len(values) < 2 {
				return nil, errors.New("insufficent arguments")
			}

			// first load the geometry from wkb string
			z := values[0].(int64)
			geomWKB := values[1].([]byte)

			geometry, err := wkb.NewDecoder(bytes.NewBuffer(geomWKB)).Decode()
			if err != nil {
				return nil, err
			}

			tileset, err := tilecover.Geometry(geometry, maptile.Zoom(z))
			if err != nil {
				return nil, err
			}

			tileIDList := make([]TileCover, 0)
			for tile := range tileset {
				tileIDList = append(tileIDList, TileCover{Z: uint64(tile.Z), X: uint64(tile.X), Y: uint64(tile.Y)})
			}

			return tileIDList, nil
		},
	}
}
