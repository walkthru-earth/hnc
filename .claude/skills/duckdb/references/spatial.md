# Spatial Analysis with DuckDB

All tools via pixi: `pixi run duckdb`.

## Step 0 -- Discover available ST_* functions (MUST run first)

Before writing any spatial query, run this to get the latest function signatures directly from the engine:

```sql
INSTALL spatial;
LOAD spatial;
SELECT function_name, function_type, return_type,
       parameters, parameter_types, description
FROM duckdb_functions()
WHERE function_name ILIKE 'ST_%'
ORDER BY function_name, return_type;
```

This is the **authoritative source**. It reflects the exact version installed, including any new functions added in updates. The spatial extension provides built-in descriptions for most functions. To look up a specific function: `AND function_name = 'ST_Transform'`.

## Gotchas

- **CRS must be VARCHAR, never INTEGER**: `ST_SetCRS(geom, 'EPSG:4326')` works, `ST_SetCRS(geom, 4326)` fails.
- **ST_Transform requires both source AND target CRS**: `ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')`.
- **ST_Transform axis order**: DuckDB defaults to CRS-defined axis order (EPSG:4326 = lat,lon). Set `geometry_always_xy = true` before any ST_Transform call for consistent lon,lat (X,Y) order.
- **Distance on EPSG:4326 returns degrees**, not meters. Use `ST_Distance_Sphere` (haversine, fast) or `ST_Distance_Spheroid` (ellipsoidal, accurate). Both are points-only and expect WGS84 with **latitude, longitude** axis order.
- **ST_Read vs read_parquet**: Use `ST_Read` for spatial files (GeoJSON, GPKG, Shapefile). Use `read_parquet` for GeoParquet (it preserves geometry natively).
- **ST_Hilbert** is for spatial sorting/indexing, not the Hilbert curve visualization.
- **MVT functions** (`ST_AsMVT`, `ST_AsMVTGeom`) require projected coordinates.

## Function categories (113+ functions)

| Category | Key functions | Notes |
|----------|--------------|-------|
| **Constructors** | `ST_Point`, `ST_MakeLine`, `ST_MakePolygon`, `ST_MakeEnvelope`, `ST_Collect`, `ST_Multi` | Build geometries from coordinates |
| **Serialization** | `ST_GeomFromText`, `ST_GeomFromGeoJSON`, `ST_AsGeoJSON`, `ST_AsHEXWKB`, `ST_AsSVG` | WKT/WKB/GeoJSON I/O |
| **Measurement** | `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Perimeter` | `_Spheroid`/`_Sphere` variants for geodesic accuracy |
| **Predicates** | `ST_Contains`, `ST_Intersects`, `ST_Within`, `ST_Crosses`, `ST_Touches`, `ST_DWithin`, `ST_Overlaps`, `ST_Equals` | Spatial relationships |
| **Operations** | `ST_Buffer`, `ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_Simplify`, `ST_ConvexHull`, `ST_ConcaveHull`, `ST_BuildArea` | Geometry manipulation |
| **Coordinates** | `ST_X`, `ST_Y`, `ST_Z`, `ST_M`, `ST_XMin/Max`, `ST_YMin/Max`, `ST_ZMin/Max` | Coordinate extraction |
| **Transform** | `ST_Transform`, `ST_FlipCoordinates`, `ST_Force2D/3DZ/3DM/4D`, `ST_Rotate`, `ST_Scale`, `ST_Translate` | CRS and geometric transforms |
| **Line ops** | `ST_LineInterpolatePoint`, `ST_LineLocatePoint`, `ST_LineSubstring`, `ST_LineMerge`, `ST_ShortestLine` | Linear referencing |
| **Indexing** | `ST_Hilbert`, `ST_QuadKey`, `ST_TileEnvelope` | Spatial sorting and tiling |
| **Coverage** | `ST_CoverageUnion`, `ST_CoverageSimplify`, `ST_CoverageInvalidEdges` + `_Agg` variants | Topological coverage ops |
| **I/O** | `ST_Read`, `ST_ReadOSM`, `ST_ReadSHP`, `ST_Read_Meta`, `ST_Drivers` | File reading (GeoJSON, GPKG, SHP, OSM) |
| **MVT** | `ST_AsMVT`, `ST_AsMVTGeom` | Mapbox vector tiles |
| **Aggregates** | `ST_Union_Agg`, `ST_Extent_Agg`, `ST_Intersection_Agg`, `ST_MemUnion_Agg`, `ST_Collect` | Geometry aggregation |
| **Validation** | `ST_IsValid`, `ST_IsSimple`, `ST_IsRing`, `ST_IsClosed`, `ST_IsEmpty`, `ST_MakeValid` | Geometry checks and repair |

## Common patterns

```sql
-- Load and read
LOAD spatial;
SELECT * FROM ST_Read('file.gpkg');

-- CRS transform
SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:3857') FROM my_table;

-- Spatial join
SELECT a.*, b.name FROM polygons a, points b
WHERE ST_Contains(a.geom, b.geom);

-- Aggregation
SELECT region, ST_Union_Agg(geom) AS merged FROM parcels GROUP BY region;

-- GeoParquet output
COPY (SELECT * FROM my_spatial_table) TO 'out.parquet' (FORMAT PARQUET);
```

## Analysis patterns

- Distance: use projected CRS (not EPSG:4326) for metric accuracy
- Large-scale points: H3, S2, or A5 indexing (see **h3**, **geography**, **a5** references)
- For GDAL CLI operations (format conversion, reprojection, raster/terrain): see the **gdal** skill
- For GeoParquet optimization (Hilbert sorting, bbox covering, validation): see the **geoparquet** skill
