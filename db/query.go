package db

var QUERY = `
with r as (
	select geom, osm_id as id, ogc_fid as fid from buildings b
)
select ST_AsGeoJSON(r.*) from  r; 
`