package queries

// load the spatial extension
const INITIALIZE_DB = `
LOAD spatial;

-- optionally load the data from the parquet file into the database

-- CREATE TABLE features AS (SELECT * FROM read_parquet('./path-to-parquet'));
`

const GENERATE_TILE_COVERS = `
SELECT osm_id as id, geom as geometry FROM features
`