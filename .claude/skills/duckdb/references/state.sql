-- ai-data-registry DuckDB state
INSTALL spatial; LOAD spatial;
INSTALL httpfs; LOAD httpfs;
INSTALL fts; LOAD fts;

-- ArcGIS macros: .read .duckdb-skills/arcgis.sql
-- (not auto-loaded, load on demand to keep state.sql fast)
