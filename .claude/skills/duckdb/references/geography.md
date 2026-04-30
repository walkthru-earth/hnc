# Geography Extension for DuckDB (S2 Spherical Geometry)

Spherical geometry on the WGS84 ellipsoid using [Google's S2 library](https://github.com/google/s2geometry). Edges are geodesics (not planar). All distances/areas in meters. No CRS transforms needed. Matches [BigQuery Geography](https://cloud.google.com/bigquery/docs/geospatial-data#coordinate_systems_and_edges) semantics.

Source: [paleolimbot/duckdb-geography](https://github.com/paleolimbot/duckdb-geography)

## Step 0 -- Discover available s2_* functions (MUST run first)

Before writing any geography query, run this to get the latest function signatures directly from the engine:

```sql
INSTALL geography FROM community;
LOAD geography;
SELECT function_name, function_type, return_type, parameters, parameter_types
FROM duckdb_functions()
WHERE function_name LIKE 's2_%'
ORDER BY function_name;
```

This is the **authoritative source**. It reflects the exact version installed, including parameter types and custom return types (GEOGRAPHY, S2_CELL, S2_BOX, etc.). The geography extension does not provide built-in `description` values, so use the category guide below for semantics, then verify exact signatures with the query above. To look up a specific function: `AND function_name = 's2_distance'`.

## vs `spatial` extension

- `spatial` (ST_*): Planar/Cartesian geometry, requires CRS management
- `geography` (s2_*): Spherical geometry, geodesic edges, meters everywhere, no CRS needed

Use `geography` when you need accurate distances/areas on the globe without projecting. Use `spatial` for projected coordinate systems or when you need the full ST_* function set.

## Gotchas

- **Geography uses LON, LAT order**: `s2_cellfromlonlat(longitude, latitude)`. Standard GIS order.
- **Implicit WKT cast**: `'POINT (-74 40.7)'::GEOGRAPHY` works directly, no need for `s2_geogfromtext`.
- **All types implicitly cast to GEOGRAPHY**: S2_CELL, S2_CELL_CENTER, S2_CELL_UNION, S2_BOX can all be passed to geography functions.
- **`s2_cellfromlonlat` returns level-30 cells** (leaf level, ~2cm precision). There is no resolution parameter.
- **`s2_cell_level` returns TINYINT** (not INTEGER like other extensions).
- **`s2_prepare`** is critical for performance when doing repeated predicate/overlay ops on the same geometry.
- **`s2_mayintersect`** is a fast approximate check (can return false positives but never false negatives).
- **Sample data is limited**: `s2_data_countries()` and `s2_data_cities()` contain major entries only, not all countries/cities. Invalid names throw errors.
- **Parameter names are generic** (`col0, col1, ...`) in `duckdb_functions()`. Rely on the category guide for semantics.

## Custom types

| Type | Storage | What it is |
|------|---------|------------|
| `GEOGRAPHY` | BLOB | Points, lines, polygons with geodesic edges. Implicit cast from WKT strings. |
| `S2_CELL` | UBIGINT | Hierarchical cell index (31 levels, ~2cm leaf precision) |
| `S2_CELL_CENTER` | UBIGINT | Center of an S2_CELL (compact 8-byte point) |
| `S2_CELL_UNION` | LIST | Normalized cell list for coverings/bounds |
| `S2_BOX` | STRUCT | Bounding box (xmin, ymin, xmax, ymax) |

All types are implicitly castable to `GEOGRAPHY`.

## Function categories (57 functions)

| Category | Key functions | Notes |
|----------|--------------|-------|
| **Measurement** | `s2_area`, `s2_length`, `s2_perimeter`, `s2_distance`, `s2_max_distance`, `s2_dwithin` | All return meters or m2 |
| **Accessors** | `s2_x`, `s2_y`, `s2_dimension`, `s2_num_points`, `s2_isempty` | Coordinate extraction, metadata |
| **Predicates** | `s2_contains`, `s2_intersects`, `s2_equals`, `s2_mayintersect` | `s2_mayintersect` is a fast approximate check |
| **Validation** | `s2_is_valid`, `s2_is_valid_reason` | Check geometry validity |
| **Overlay** | `s2_intersection`, `s2_union`, `s2_difference` | Boolean operations on geographies |
| **Serialization** | `s2_geogfromtext`, `s2_geogfromwkb`, `s2_astext`, `s2_aswkb`, `s2_format` | WKT/WKB I/O. `_novalidate` variants skip checks |
| **Performance** | `s2_prepare` | Prepare for faster repeated predicate/overlay ops |
| **Bounding box** | `s2_bounds_box`, `s2_box`, `s2_box_intersects`, `s2_box_union`, `s2_box_struct`, `s2_box_wkb` | S2_BOX operations |
| **Coverings** | `s2_covering`, `s2_covering_fixed_level` | S2 cell coverings for spatial indexing |
| **S2 cell ops** | `s2_cellfromlonlat`, `s2_cellfromwkb`, `s2_cell_level`, `s2_cell_parent`, `s2_cell_child`, `s2_cell_contains`, `s2_cell_intersects`, `s2_cell_edge_neighbor`, `s2_cell_token`, `s2_cell_from_token`, `s2_cell_range_min/max`, `s2_cell_vertex` | Hierarchical cell index |
| **Sample data** | `s2_data_countries()`, `s2_data_cities()`, `s2_data_country(name)`, `s2_data_city(name)` | Built-in table functions |
| **Aggregates** | `s2_bounds_box_agg` | Aggregate bounding box |

## Common patterns

```sql
-- Spatial join with built-in data
SELECT countries.name AS country, cities.name AS city
FROM s2_data_countries() AS countries
INNER JOIN s2_data_cities() AS cities
  ON s2_intersects(countries.geog, cities.geog);

-- Distance in meters
SELECT s2_distance(s2_data_city('Vancouver'), s2_data_city('Seattle')) AS dist_m;

-- Compact point storage (8 bytes per point)
SELECT s2_cellfromlonlat(longitude, latitude) AS cell FROM my_points;

-- Prepare for repeated joins (index acceleration)
CREATE TABLE countries AS
  SELECT name, s2_prepare(geog) AS geog FROM s2_data_countries();

-- S2 cell covering for spatial indexing
SELECT name, s2_covering(geog) AS covering FROM s2_data_countries() LIMIT 5;

-- WKT cast shorthand
SELECT 'POINT (-74 40.7)'::GEOGRAPHY;
```
