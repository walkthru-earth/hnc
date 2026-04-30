-- =============================================================================
-- ArcGIS REST Macros for DuckDB (v1.5.1+, VARIANT-optimized)
-- =============================================================================
-- Swiss-knife macros for ArcGIS FeatureServer, MapServer, and REST services.
-- Uses VARIANT (typed binary) instead of JSON text for metadata access.
-- Uses json_each() + VARIANT cast for array iteration (replaces generate_series).
--
-- Levels: L0 (primitives) -> L1 (discovery + schema) -> L2 (data) -> L3 (patterns)
--
-- Usage:
--   pixi run duckdb -init ".duckdb-skills/arcgis.sql"
--   .read .duckdb-skills/arcgis.sql
--
-- Authentication (pick one, run before queries):
--   SET VARIABLE arcgis_token = 'YOUR_TOKEN';
--   CREATE SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'YOUR_TOKEN');
--   CREATE SECRET arcgis_auth (TYPE HTTP, EXTRA_HTTP_HEADERS MAP {
--       'X-Esri-Authorization': 'Bearer YOUR_TOKEN'});
--
-- =============================================================================

INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;
SET geometry_always_xy = true;

-- Default session variables (safe to call multiple times)
SET VARIABLE arcgis_token = '';
SET VARIABLE arcgis_crs = 'EPSG:4326';
SET VARIABLE arcgis_proxy = '';

-- =============================================================================
-- L0: PRIMITIVES (type mapping, URL builders)
-- =============================================================================

-- Map Esri field types to DuckDB types (16 types).
CREATE OR REPLACE MACRO arcgis_type_map(esri_type) AS
    CASE esri_type
        WHEN 'esriFieldTypeOID'             THEN 'INTEGER'
        WHEN 'esriFieldTypeSmallInteger'    THEN 'SMALLINT'
        WHEN 'esriFieldTypeInteger'         THEN 'INTEGER'
        WHEN 'esriFieldTypeBigInteger'      THEN 'BIGINT'
        WHEN 'esriFieldTypeSingle'          THEN 'FLOAT'
        WHEN 'esriFieldTypeDouble'          THEN 'DOUBLE'
        WHEN 'esriFieldTypeString'          THEN 'VARCHAR'
        WHEN 'esriFieldTypeDate'            THEN 'TIMESTAMP'
        WHEN 'esriFieldTypeDateOnly'        THEN 'DATE'
        WHEN 'esriFieldTypeTimeOnly'        THEN 'TIME'
        WHEN 'esriFieldTypeTimestampOffset' THEN 'TIMESTAMPTZ'
        WHEN 'esriFieldTypeGUID'            THEN 'UUID'
        WHEN 'esriFieldTypeGlobalID'        THEN 'UUID'
        WHEN 'esriFieldTypeXML'             THEN 'VARCHAR'
        WHEN 'esriFieldTypeBlob'            THEN 'BLOB'
        WHEN 'esriFieldTypeGeometry'        THEN 'GEOMETRY'
        ELSE 'VARCHAR'
    END;

-- Map Esri geometry types to WKT names.
CREATE OR REPLACE MACRO arcgis_geom_map(esri_geom) AS
    CASE esri_geom
        WHEN 'esriGeometryPoint'      THEN 'POINT'
        WHEN 'esriGeometryMultipoint' THEN 'MULTIPOINT'
        WHEN 'esriGeometryPolyline'   THEN 'LINESTRING'
        WHEN 'esriGeometryPolygon'    THEN 'POLYGON'
        WHEN 'esriGeometryEnvelope'   THEN 'POLYGON'
        ELSE 'GEOMETRY'
    END;

-- Token helper: appends &token= if arcgis_token variable is set.
CREATE OR REPLACE MACRO _arcgis_token_suffix() AS
    CASE WHEN getvariable('arcgis_token') != ''
         THEN '&token=' || getvariable('arcgis_token') ELSE '' END;

-- Proxy helper: prepends proxy prefix to URL when arcgis_proxy is set.
-- Pattern: proxy.ashx?{real_url} — the real URL becomes the query string.
-- Used by: all macros that fetch URLs (read_text, read_json_auto).
-- Set up: SET VARIABLE arcgis_proxy = 'https://maps.example.com/proxy/proxy.ashx?';
--          CREATE SECRET proxy_auth (TYPE HTTP,
--              EXTRA_HTTP_HEADERS MAP {'Referer': 'https://maps.example.com/'},
--              SCOPE 'https://maps.example.com/');
CREATE OR REPLACE MACRO _arcgis_apply_proxy(url) AS
    CASE WHEN getvariable('arcgis_proxy') != ''
         THEN getvariable('arcgis_proxy') || url
         ELSE url END;

-- List variant: apply proxy to each URL in a list (for paginated reads).
CREATE OR REPLACE MACRO _arcgis_apply_proxy_list(urls) AS
    CASE WHEN getvariable('arcgis_proxy') != ''
         THEN [getvariable('arcgis_proxy') || u FOR u IN urls]
         ELSE urls END;

-- Build a GeoJSON query URL with pagination and auth.
-- Adds orderByFields=OBJECTID+ASC by default for reliable pagination.
CREATE OR REPLACE MACRO arcgis_query_url(
    base_url,
    layer_id,
    where_clause := '1%3D1',
    out_sr := '4326',
    out_fields := '%2A',
    page_size := NULL,
    result_offset := NULL,
    order_by := 'OBJECTID+ASC'
) AS
    base_url || '/' || layer_id || '/query?where=' || where_clause
    || '&outFields=' || out_fields || '&outSR=' || out_sr || '&returnGeometry=true'
    || '&orderByFields=' || order_by
    || CASE WHEN page_size IS NOT NULL THEN '&resultRecordCount=' || page_size ELSE '' END
    || CASE WHEN result_offset IS NOT NULL THEN '&resultOffset=' || result_offset ELSE '' END
    || '&f=geojson' || _arcgis_token_suffix();

-- Build a metadata URL (?f=json).
CREATE OR REPLACE MACRO arcgis_meta_url(base_url, layer_id) AS
    base_url || '/' || layer_id || '?f=json' || _arcgis_token_suffix();

-- =============================================================================
-- L1: DISCOVERY (catalog, service, layer enumeration)
-- =============================================================================

-- List all folders and services in a REST catalog.
-- Input: 'https://server/arcgis/rest/services?f=json'
CREATE OR REPLACE MACRO arcgis_catalog(catalog_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(_arcgis_apply_proxy(catalog_url))
    ),
    folders AS (
        SELECT 'folder' AS item_type,
               value->>'$' AS name,
               NULL::VARCHAR AS service_type
        FROM raw, json_each(raw.j, '$.folders')
    ),
    services AS (
        SELECT 'service' AS item_type,
               (value::VARIANT).name::VARCHAR AS name,
               (value::VARIANT).type::VARCHAR AS service_type
        FROM raw, json_each(raw.j, '$.services')
    )
    SELECT * FROM (SELECT * FROM folders UNION ALL SELECT * FROM services);

-- List only FeatureServer/MapServer services (convenience filter).
CREATE OR REPLACE MACRO arcgis_services(catalog_url) AS TABLE
    SELECT name AS service_name, service_type
    FROM arcgis_catalog(catalog_url)
    WHERE service_type IN ('FeatureServer', 'MapServer');

-- List all layers and tables in a service.
-- Input: 'https://server/.../FeatureServer?f=json'
CREATE OR REPLACE MACRO arcgis_layers(service_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(_arcgis_apply_proxy(service_url))
    ),
    layers AS (
        SELECT (value::VARIANT).id::INTEGER AS layer_id,
               (value::VARIANT).name::VARCHAR AS layer_name,
               (value::VARIANT).geometryType::VARCHAR AS geometry_type,
               'layer' AS item_type
        FROM raw, json_each(raw.j, '$.layers')
    ),
    tbls AS (
        SELECT (value::VARIANT).id::INTEGER AS layer_id,
               (value::VARIANT).name::VARCHAR AS layer_name,
               NULL::VARCHAR AS geometry_type,
               'table' AS item_type
        FROM raw, json_each(raw.j, '$.tables')
    )
    SELECT * FROM (SELECT * FROM layers UNION ALL SELECT * FROM tbls)
    ORDER BY layer_id;

-- =============================================================================
-- L1: SCHEMA INSPECTION (metadata, fields, domains, subtypes, relationships)
-- =============================================================================

-- Full layer metadata as VARIANT for dot-notation exploration.
-- Example: SELECT meta.name, meta.extent.spatialReference.latestWkid,
--          meta.advancedQueryCapabilities.supportsPagination
--          FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json');
CREATE OR REPLACE MACRO arcgis_layer_meta(layer_url) AS TABLE
    SELECT (content::JSON)::VARIANT AS meta
    FROM read_text(_arcgis_apply_proxy(layer_url));

-- One-row structured summary (typed columns, not VARIANT).
CREATE OR REPLACE MACRO arcgis_meta(layer_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(layer_url))
    )
    SELECT v.name::VARCHAR                    AS name,
           v.type::VARCHAR                    AS layer_type,
           v.geometryType::VARCHAR            AS geometry_type,
           arcgis_geom_map(v.geometryType::VARCHAR) AS duckdb_geom_type,
           v.maxRecordCount::INTEGER          AS max_records,
           COALESCE(v.extent.spatialReference.latestWkid,
                    v.extent.spatialReference.wkid)::INTEGER AS wkid,
           v.objectIdField::VARCHAR           AS oid_field,
           v.hasAttachments::BOOLEAN          AS has_attachments,
           v.advancedQueryCapabilities.supportsPagination::BOOLEAN   AS pagination,
           v.advancedQueryCapabilities.supportsStatistics::BOOLEAN   AS statistics,
           v.advancedQueryCapabilities.supportsOrderBy::BOOLEAN      AS order_by,
           v.advancedQueryCapabilities.supportsDistinct::BOOLEAN     AS distinct_vals,
           v.advancedQueryCapabilities.supportsReturningGeometryCentroid::BOOLEAN AS centroids,
           v.supportedQueryFormats::VARCHAR   AS query_formats,
           json_array_length((v.fields)::JSON)::INTEGER AS field_count,
           COALESCE(json_array_length((v.relationships)::JSON), 0)::INTEGER AS rel_count,
           COALESCE(json_array_length((v.types)::JSON), 0)::INTEGER AS subtype_count,
           v.description::VARCHAR             AS description
    FROM raw;

-- Feature count via server-side returnCountOnly.
-- Input: base query URL (without returnCountOnly/f params, but with where=).
CREATE OR REPLACE MACRO arcgis_count(query_url) AS TABLE
    SELECT ((content::JSON)::VARIANT).count::INTEGER AS total
    FROM read_text(_arcgis_apply_proxy(query_url || '&returnCountOnly=true&f=json'));

-- Object IDs only (fast, for pagination planning).
CREATE OR REPLACE MACRO arcgis_ids(query_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(query_url || '&returnIdsOnly=true&f=json'))
    )
    SELECT v.objectIdFieldName::VARCHAR AS oid_field,
           json_array_length((v.objectIds)::JSON)::INTEGER AS id_count
    FROM raw;

-- Layer extent as geometry (for spatial filtering, bbox previews).
CREATE OR REPLACE MACRO arcgis_extent(layer_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(layer_url))
    )
    SELECT v.extent.xmin::DOUBLE AS xmin,
           v.extent.ymin::DOUBLE AS ymin,
           v.extent.xmax::DOUBLE AS xmax,
           v.extent.ymax::DOUBLE AS ymax,
           COALESCE(v.extent.spatialReference.latestWkid,
                    v.extent.spatialReference.wkid)::INTEGER AS wkid,
           ST_MakeEnvelope(
               v.extent.xmin::DOUBLE, v.extent.ymin::DOUBLE,
               v.extent.xmax::DOUBLE, v.extent.ymax::DOUBLE
           ) AS extent_geom
    FROM raw;

-- All fields with Esri type, DuckDB type mapping, alias, domain info.
-- Uses json_each + VARIANT for clean array iteration.
CREATE OR REPLACE MACRO arcgis_fields(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(_arcgis_apply_proxy(layer_url))
    )
    SELECT (value::VARIANT).name::VARCHAR                            AS field_name,
           (value::VARIANT).alias::VARCHAR                           AS field_alias,
           (value::VARIANT).type::VARCHAR                            AS esri_type,
           arcgis_type_map((value::VARIANT).type::VARCHAR)           AS duckdb_type,
           (value::VARIANT).domain.type::VARCHAR                     AS domain_type,
           (value::VARIANT).domain.name::VARCHAR                     AS domain_name,
           (value::VARIANT).length::INTEGER                          AS field_length,
           (value::VARIANT).nullable::BOOLEAN                        AS nullable,
           (value::VARIANT).editable::BOOLEAN                        AS editable,
           (value::VARIANT).defaultValue                             AS default_value
    FROM raw, json_each(raw.j, '$.fields');

-- Coded value domain lookups (one row per code/label pair).
-- Uses nested json_each: outer for fields, inner for codedValues.
CREATE OR REPLACE MACRO arcgis_domains(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(_arcgis_apply_proxy(layer_url))
    ),
    fields_with_domains AS (
        SELECT (value::VARIANT).name::VARCHAR AS field_name,
               (value::VARIANT).domain AS dom
        FROM raw, json_each(raw.j, '$.fields')
        WHERE (value::VARIANT).domain.type::VARCHAR = 'codedValue'
    )
    SELECT field_name,
           (cv::VARIANT).code AS code,
           (cv::VARIANT).name::VARCHAR AS label
    FROM fields_with_domains,
         json_each(dom::JSON, '$.codedValues') AS t(k, cv);

-- Subtypes (type field + id/name pairs).
CREATE OR REPLACE MACRO arcgis_subtypes(layer_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v,
               content::JSON AS j
        FROM read_text(_arcgis_apply_proxy(layer_url))
    )
    SELECT v.typeIdField::VARCHAR AS type_field,
           (value::VARIANT).id::INTEGER AS subtype_id,
           (value::VARIANT).name::VARCHAR AS subtype_name,
           (value::VARIANT).domains AS subtype_domains
    FROM raw, json_each(raw.j, '$.types');

-- Relationship classes.
CREATE OR REPLACE MACRO arcgis_relationships(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(_arcgis_apply_proxy(layer_url))
    )
    SELECT (value::VARIANT).id::INTEGER           AS rel_id,
           (value::VARIANT).name::VARCHAR          AS rel_name,
           (value::VARIANT).relatedTableId::INTEGER AS related_table_id,
           (value::VARIANT).cardinality::VARCHAR    AS cardinality,
           (value::VARIANT).role::VARCHAR            AS role,
           (value::VARIANT).keyField::VARCHAR        AS key_field,
           (value::VARIANT).composite::BOOLEAN       AS composite
    FROM raw, json_each(raw.j, '$.relationships');

-- =============================================================================
-- L2: DATA ACCESS (features, statistics, CRS)
-- =============================================================================

-- Diagnostic: check what a URL returns (error, features, or empty).
-- Use before arcgis_read to verify a URL works. Shows error details if present.
-- Input: any ArcGIS query URL (f=json or f=geojson).
CREATE OR REPLACE MACRO arcgis_check(url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v FROM read_text(_arcgis_apply_proxy(url))
    )
    SELECT CASE
               WHEN v.error IS NOT NULL THEN 'ERROR'
               WHEN v.features IS NOT NULL THEN 'OK'
               WHEN v.type::VARCHAR = 'FeatureCollection' THEN 'OK'
               ELSE 'UNKNOWN'
           END AS status,
           COALESCE(v.error.code::INTEGER, 0) AS error_code,
           COALESCE(v.error.message::VARCHAR, '') AS error_message,
           CASE
               WHEN v.features IS NOT NULL
                   THEN json_array_length((v.features)::JSON)::INTEGER
               ELSE 0
           END AS feature_count
    FROM raw;

-- Raw response as VARIANT for debugging any URL.
-- Useful when arcgis_read fails and you need to see the actual response.
CREATE OR REPLACE MACRO arcgis_raw(url) AS TABLE
    SELECT (content::JSON)::VARIANT AS response
    FROM read_text(_arcgis_apply_proxy(url));

-- Raw feature query (geometry WITHOUT CRS tag).
CREATE OR REPLACE MACRO arcgis_query(query_url) AS TABLE
    SELECT unnest(feature.properties),
           ST_GeomFromGeoJSON(feature.geometry) AS geometry
    FROM (
        SELECT unnest(features) AS feature
        FROM read_json_auto(_arcgis_apply_proxy(query_url))
    );

-- Feature query WITH CRS (recommended for GeoParquet export).
-- Default: EPSG:4326 (override via crs param or arcgis_crs variable).
-- NOTE: Requires f=geojson in the URL. If server returns error with geojson,
-- use arcgis_read_json() with f=json as fallback.
CREATE OR REPLACE MACRO arcgis_read(query_url, crs := NULL) AS TABLE
    SELECT unnest(feature.properties),
           ST_SetCRS(
               ST_GeomFromGeoJSON(feature.geometry),
               COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326')
           ) AS geometry
    FROM (
        SELECT unnest(features) AS feature
        FROM read_json_auto(_arcgis_apply_proxy(query_url))
    );

-- Feature query from Esri JSON format (f=json).
-- Fallback for servers that don't support f=geojson.
-- Returns attributes as a JSON column + geometry. Use json_extract or ->> to get fields.
-- Handles point, multipoint, polyline, and polygon geometries.
-- ST_MakeValid + ST_Normalize fix Esri's CW ring orientation (opposite to OGC/GeoJSON CCW).
-- Example: SELECT attrs->>'NAME' AS name, geometry FROM arcgis_read_json('...&f=json');
CREATE OR REPLACE MACRO arcgis_read_json(query_url, crs := NULL) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(query_url))
    ),
    features AS (
        SELECT value::VARIANT AS f
        FROM raw, json_each((raw.v.features)::JSON)
    )
    SELECT (f.attributes)::JSON AS attrs,
           ST_Normalize(ST_MakeValid(
               CASE
                   WHEN f.geometry.x IS NOT NULL THEN
                       ST_SetCRS(ST_Point(f.geometry.x::DOUBLE, f.geometry.y::DOUBLE),
                           COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326'))
                   WHEN f.geometry.points IS NOT NULL THEN
                       ST_SetCRS(ST_GeomFromGeoJSON(
                           '{"type":"MultiPoint","coordinates":' || (f.geometry.points)::JSON::VARCHAR || '}'),
                           COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326'))
                   WHEN f.geometry.rings IS NOT NULL THEN
                       ST_SetCRS(ST_GeomFromGeoJSON(
                           '{"type":"Polygon","coordinates":' || (f.geometry.rings)::JSON::VARCHAR || '}'),
                           COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326'))
                   WHEN f.geometry.paths IS NOT NULL THEN
                       ST_SetCRS(ST_GeomFromGeoJSON(
                           '{"type":"MultiLineString","coordinates":' || (f.geometry.paths)::JSON::VARCHAR || '}'),
                           COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326'))
                   ELSE NULL
               END
           )) AS geometry
    FROM features;

-- Server-side statistics (no data transfer, computed on server).
-- Input: query_url with outStatistics param already URL-encoded.
-- Works with f=json format (stats never support geojson).
-- Returns attrs as JSON column. Use ->> to extract specific stats.
-- Example: SELECT attrs->>'total_pop' FROM arcgis_stats('...&outStatistics=...&f=json');
CREATE OR REPLACE MACRO arcgis_stats(query_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(query_url))
    ),
    features AS (
        SELECT value::VARIANT AS f
        FROM raw, json_each((raw.v.features)::JSON)
    )
    SELECT (f.attributes)::JSON AS attrs
    FROM features;

-- Server-side extent (fast bbox from query, no feature transfer).
CREATE OR REPLACE MACRO arcgis_query_extent(query_url) AS TABLE
    WITH raw AS (
        SELECT (content::JSON)::VARIANT AS v
        FROM read_text(_arcgis_apply_proxy(query_url || '&returnExtentOnly=true&f=json'))
    )
    SELECT v.extent.xmin::DOUBLE AS xmin,
           v.extent.ymin::DOUBLE AS ymin,
           v.extent.xmax::DOUBLE AS xmax,
           v.extent.ymax::DOUBLE AS ymax,
           v.count::INTEGER AS feature_count,
           ST_MakeEnvelope(
               v.extent.xmin::DOUBLE, v.extent.ymin::DOUBLE,
               v.extent.xmax::DOUBLE, v.extent.ymax::DOUBLE
           ) AS extent_geom
    FROM raw;

-- =============================================================================
-- L3: PATTERNS (copy-paste recipes for common workflows)
-- =============================================================================

-- Domain Resolution (3 steps):
--
-- Step 1: Set layer
--   SET VARIABLE arcgis_layer = 'https://.../FeatureServer/0?f=json';
--
-- Step 2: Build MAP-of-MAPs resolver + macro
--   CREATE OR REPLACE TEMP TABLE _domains AS
--   WITH dl AS (SELECT * FROM arcgis_domains(getvariable('arcgis_layer')))
--   SELECT MAP(list(field_name), list(lookup)) AS all_domains
--   FROM (SELECT field_name, MAP(list(code::VARCHAR), list(label)) AS lookup
--         FROM dl GROUP BY field_name);
--
--   CREATE OR REPLACE MACRO resolve_domain(field_val, field_name) AS
--       COALESCE(
--           (SELECT all_domains[field_name] FROM _domains)[field_val::VARCHAR],
--           (SELECT all_domains[field_name] FROM _domains)[TRY_CAST(field_val AS INTEGER)::VARCHAR]
--       );
--
-- Step 3: Use in queries
--   SELECT *, resolve_domain(material, 'material') AS material_label
--   FROM arcgis_read('https://.../query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson');

-- Pagination (datasets > maxRecordCount):
--
--   -- Get total + page size
--   SELECT total FROM arcgis_count('https://.../FeatureServer/0/query?where=1%3D1');
--   SELECT max_records FROM arcgis_meta('https://.../FeatureServer/0?f=json');
--
--   -- Paginated download (2000 per page, 10000 total)
--   SELECT unnest(feature.properties),
--          ST_SetCRS(ST_GeomFromGeoJSON(feature.geometry), 'EPSG:4326') AS geometry
--   FROM (
--       SELECT unnest(features) AS feature
--       FROM read_json_auto([
--           'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--           '&returnGeometry=true&orderByFields=OBJECTID+ASC'
--           '&resultOffset=' || x::VARCHAR || '&resultRecordCount=2000&f=geojson'
--           FOR x IN range(0, 10000, 2000)
--       ])
--   );
--
--   -- With proxy: wrap the URL list with _arcgis_apply_proxy_list
--   SELECT unnest(feature.properties),
--          ST_SetCRS(ST_GeomFromGeoJSON(feature.geometry), 'EPSG:4326') AS geometry
--   FROM (
--       SELECT unnest(features) AS feature
--       FROM read_json_auto(_arcgis_apply_proxy_list([
--           'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--           '&returnGeometry=true&orderByFields=OBJECTID+ASC'
--           '&resultOffset=' || x::VARCHAR || '&resultRecordCount=2000&f=geojson'
--           FOR x IN range(0, 10000, 2000)
--       ]))
--   );

-- Server-Side Statistics (no feature transfer):
--
--   SELECT * FROM arcgis_stats(
--       'https://.../FeatureServer/0/query?where=1%3D1'
--       '&outStatistics=%5B'
--       '%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22POP%22'
--       '%2C%22outStatisticFieldName%22%3A%22total_pop%22%7D'
--       '%5D&f=json'
--   );
--
--   -- With GROUP BY (server-side):
--   SELECT * FROM arcgis_stats(
--       'https://.../FeatureServer/0/query?where=1%3D1'
--       '&groupByFieldsForStatistics=STATE_NAME'
--       '&outStatistics=%5B%7B%22statisticType%22%3A%22count%22'
--       '%2C%22onStatisticField%22%3A%22OBJECTID%22'
--       '%2C%22outStatisticFieldName%22%3A%22cnt%22%7D%5D&f=json'
--   );

-- GeoParquet Export with VARIANT Metadata:
--
--   COPY (
--       WITH lm AS (
--           SELECT meta FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json')
--       )
--       SELECT f.*,
--              (SELECT meta.drawingInfo FROM lm)::VARIANT AS drawing_info,
--              (SELECT meta.fields FROM lm)::VARIANT AS fields_schema,
--              (SELECT meta.relationships FROM lm)::VARIANT AS relationships
--       FROM arcgis_read(
--           'https://.../query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson'
--       ) f
--   ) TO 'output.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000);

-- Spatial Filter (bbox):
--
--   SELECT * FROM arcgis_read(
--       'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--       '&returnGeometry=true'
--       '&geometry=-122.5,37.5,-122.0,38.0'
--       '&geometryType=esriGeometryEnvelope'
--       '&inSR=4326&spatialRel=esriSpatialRelIntersects'
--       '&f=geojson'
--   );

-- Reverse Proxy (proxy.ashx-style):
--
--   -- Step 1: Set proxy prefix (trailing ? is required)
--   SET VARIABLE arcgis_proxy = 'https://maps.example.com/proxy/proxy.ashx?';
--
--   -- Step 2: Set Referer header (scoped to proxy domain)
--   CREATE SECRET proxy_referer (TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'Referer': 'https://maps.example.com/'},
--       SCOPE 'https://maps.example.com/');
--
--   -- Step 3: Use macros normally — proxy is applied automatically
--   SELECT * FROM arcgis_layers(
--       'https://gis-backend.example.com/server/rest/services/MyService/MapServer?f=json');
--   SELECT * FROM arcgis_meta(
--       'https://gis-backend.example.com/server/rest/services/MyService/MapServer/0?f=json');
--   SELECT * FROM arcgis_read(
--       'https://gis-backend.example.com/server/rest/services/MyService/MapServer/0'
--       '/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson');
--
--   -- Step 4: Disable proxy when done (or for direct servers)
--   SET VARIABLE arcgis_proxy = '';
--
--   -- Combine with token auth if needed:
--   SET VARIABLE arcgis_proxy = 'https://maps.example.com/proxy/proxy.ashx?';
--   SET VARIABLE arcgis_token = 'YOUR_TOKEN';
--   CREATE SECRET proxy_auth (TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'Referer': 'https://maps.example.com/'},
--       SCOPE 'https://maps.example.com/');

-- Point + Buffer:
--
--   SELECT * FROM arcgis_read(
--       'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--       '&returnGeometry=true'
--       '&geometry=-122.42,37.78'
--       '&geometryType=esriGeometryPoint&inSR=4326'
--       '&spatialRel=esriSpatialRelIntersects'
--       '&distance=500&units=esriSRUnit_Meter'
--       '&f=geojson'
--   );

-- =============================================================================
-- AUTHENTICATION REFERENCE (comprehensive Esri auth support)
-- =============================================================================
--
-- Method 1: Token as URL parameter (simplest, works with arcgis_query_url)
--   SET VARIABLE arcgis_token = 'YOUR_TOKEN';
--
-- Method 2: Standard Bearer token (recommended for ArcGIS Online)
--   CREATE SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'YOUR_TOKEN');
--
-- Method 3: X-Esri-Authorization header (for Enterprise with web-tier auth)
--   CREATE SECRET arcgis_auth (
--       TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'X-Esri-Authorization': 'Bearer YOUR_TOKEN'});
--
-- Method 4: API Key (long-lived, same as token param)
--   SET VARIABLE arcgis_token = 'AAPT85fOqy...your_api_key';
--
-- Method 5: Scoped secrets (different tokens per server)
--   CREATE SECRET agol (TYPE HTTP, BEARER_TOKEN 'online_token',
--       SCOPE 'https://services1.arcgis.com/');
--   CREATE SECRET enterprise (TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'X-Esri-Authorization': 'Bearer ent_token'},
--       SCOPE 'https://gis.mycompany.com/');
--
-- Method 6: Bearer + Referer (servers that require Referer header)
--   CREATE SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'TOKEN',
--       EXTRA_HTTP_HEADERS MAP {'Referer': 'https://myapp.example.com'});
--
-- Method 7: Behind corporate proxy
--   CREATE SECRET arcgis_proxy (TYPE HTTP, BEARER_TOKEN 'TOKEN',
--       HTTP_PROXY 'http://proxy.corp.example.com:8080',
--       HTTP_PROXY_USERNAME 'proxyuser', HTTP_PROXY_PASSWORD 'proxypass');
--
-- Method 8: Persistent secret (survives restarts, stored at ~/.duckdb/stored_secrets)
--   CREATE PERSISTENT SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'TOKEN',
--       SCOPE 'https://services1.arcgis.com/');
--
-- Method 9: Generate token from Enterprise (requires http_client community ext)
--   INSTALL http_client FROM community; LOAD http_client;
--   SET VARIABLE arcgis_token = (
--       SELECT ((http_post_form(
--           'https://gis.mycompany.com/portal/sharing/rest/generateToken',
--           MAP {'username': 'user', 'password': 'pass',
--                'client': 'referer', 'referer': 'https://myapp.example.com',
--                'expiration': '60', 'f': 'json'},
--           MAP {}
--       )).body::JSON->>'token')
--   );
--
-- Method 10: OAuth client_credentials (requires http_client community ext)
--   INSTALL http_client FROM community; LOAD http_client;
--   CREATE SECRET arcgis_oauth (TYPE HTTP, BEARER_TOKEN (
--       SELECT ((http_post_form(
--           'https://www.arcgis.com/sharing/rest/oauth2/token',
--           MAP {'client_id': 'YOUR_CLIENT_ID', 'client_secret': 'YOUR_SECRET',
--                'grant_type': 'client_credentials', 'f': 'json'},
--           MAP {}
--       )).body::JSON->>'access_token')
--   ));
--
-- Method 11: Reverse proxy prefix (proxy.ashx-style)
--   The proxy URL is prepended to the real ArcGIS URL. The real URL becomes
--   the query string of the proxy endpoint. Typically requires a Referer header.
--   SET VARIABLE arcgis_proxy = 'https://maps.example.com/proxy/proxy.ashx?';
--   CREATE SECRET proxy_referer (TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'Referer': 'https://maps.example.com/'},
--       SCOPE 'https://maps.example.com/');
--   -- Now all macros auto-prepend the proxy:
--   SELECT * FROM arcgis_layers(
--       'https://gis-backend.example.com/server/rest/services/MyService/MapServer?f=json');
--   -- Fetches: https://maps.example.com/proxy/proxy.ashx?https://gis-backend.example.com/...
--   -- To disable: SET VARIABLE arcgis_proxy = '';
--
-- NOT SUPPORTED (interactive/system-level, use workaround):
--   IWA (Integrated Windows Auth) - generate token via browser, use Method 1-3
--   PKI (Client Certificate)     - generate token via cert-aware tool, use Method 1-3
--   SAML/SSO                     - generate token via IdP login, use Method 1-3
--   OAuth authorization_code     - browser redirect flow, use Method 1-3 with token
--
-- Verify which secret DuckDB will use for a URL:
--   SELECT * FROM which_secret('https://services1.arcgis.com/abc/rest/...', 'http');

-- =============================================================================
-- QUICK REFERENCE
-- =============================================================================
--
-- L0 Primitives:
--   arcgis_type_map(esri_type)            -> DuckDB type name (VARCHAR)
--   arcgis_geom_map(esri_geom)            -> WKT geometry type (VARCHAR)
--   arcgis_query_url(base, layer, ...)     -> full query URL (VARCHAR)
--   arcgis_meta_url(base, layer)           -> metadata URL (VARCHAR)
--   _arcgis_apply_proxy(url)              -> proxy-prefixed URL (scalar, internal)
--   _arcgis_apply_proxy_list(urls)         -> proxy-prefixed URLs (list, for pagination)
--
-- L1 Discovery:
--   arcgis_catalog(catalog_url)            -> TABLE (item_type, name, service_type)
--   arcgis_services(catalog_url)           -> TABLE (service_name, service_type)
--   arcgis_layers(service_url)             -> TABLE (layer_id, layer_name, geometry_type, item_type)
--
-- L1 Schema:
--   arcgis_layer_meta(layer_url)           -> TABLE (meta VARIANT) -- dot notation
--   arcgis_meta(layer_url)                 -> TABLE (one-row typed summary)
--   arcgis_count(query_url)                -> TABLE (total INTEGER)
--   arcgis_ids(query_url)                  -> TABLE (oid_field, id_count)
--   arcgis_extent(layer_url)               -> TABLE (xmin..ymax, wkid, extent_geom)
--   arcgis_fields(layer_url)               -> TABLE (field_name, esri_type, duckdb_type, ...)
--   arcgis_domains(layer_url)              -> TABLE (field_name, code, label)
--   arcgis_subtypes(layer_url)             -> TABLE (type_field, subtype_id, subtype_name, ...)
--   arcgis_relationships(layer_url)        -> TABLE (rel_id, rel_name, cardinality, ...)
--
-- L2 Data:
--   arcgis_check(url)                      -> TABLE (status, error_code, error_message, feature_count)
--   arcgis_raw(url)                        -> TABLE (response VARIANT) -- for debugging
--   arcgis_query(query_url)                -> TABLE (properties..., geometry) -- f=geojson
--   arcgis_read(query_url, crs)            -> TABLE (properties..., geometry WITH CRS) -- f=geojson
--   arcgis_read_json(query_url, crs)       -> TABLE (attrs JSON, geometry) -- f=json fallback
--   arcgis_stats(query_url)                -> TABLE (attrs JSON) -- f=json, server-side stats
--   arcgis_query_extent(query_url)         -> TABLE (xmin..ymax, feature_count, extent_geom)
