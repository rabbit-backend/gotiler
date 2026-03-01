package queries

// load the spatial extension
const INITIALIZE_DB = `
LOAD spatial;

-- optionally load the data from the parquet file into the database
-- CREATE TABLE features AS (SELECT * FROM read_parquet('./path-to-parquet'));

CREATE TABLE IF NOT EXISTS tile_covers (
	id 		BIGINT,
	Z 		INT,
	X 		INT,
	Y 		INT
);

DELETE FROM tile_covers;
`

const GENERATE_TILE_COVERS_FOR_ZOOM = `
INSERT INTO tile_covers
	SELECT  
		osm_id as id,
		UNNEST(ST_TileCover(?, ST_AsWKB(geom)), recursive := true)
	FROM features
`

const TOTAL_FEATURES = `SELECT COUNT(*) FROM features`

const GENERATE_MVT_GEOM = `
WITH temp_tiles AS (
	SELECT 
		ST_SimplifyPreserveTopology(
			geom, ?
		) geom,
		t.Z, t.X, t.Y
	FROM tile_covers t JOIN features f
	ON t.id = f.osm_id 
	WHERE Z = ?
)
SELECT 
	Z, 
	X, 
	Y,
	ST_AsWKB(geom)
FROM temp_tiles
`