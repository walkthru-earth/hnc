# ArcGIS REST Services via DuckDB (VARIANT-optimized)

Swiss-knife macros for ArcGIS FeatureServer, MapServer, and REST services. Uses VARIANT (typed binary) for metadata access and `json_each()` + VARIANT cast for clean array iteration. Load once per session:

```bash
pixi run duckdb -init ".claude/skills/duckdb/references/arcgis.sql"
```

Or inside a running session: `.read .claude/skills/duckdb/references/arcgis.sql`

## Macro Quick Reference

| Level | Macro | Returns |
|-------|-------|---------|
| **L0** | `arcgis_type_map(esri_type)` | DuckDB type name (16 Esri types) |
| **L0** | `arcgis_geom_map(esri_geom)` | WKT geometry type |
| **L0** | `arcgis_query_url(base, layer, ...)` | Full query URL (token, pagination, orderBy) |
| **L1** | `arcgis_catalog(catalog_url)` | TABLE: item_type, name, service_type |
| **L1** | `arcgis_services(catalog_url)` | TABLE: service_name, service_type |
| **L1** | `arcgis_layers(service_url)` | TABLE: layer_id, layer_name, geometry_type, item_type |
| **L1** | `arcgis_layer_meta(layer_url)` | TABLE: meta as VARIANT (dot notation) |
| **L1** | `arcgis_meta(layer_url)` | TABLE: one-row typed summary (18 columns) |
| **L1** | `arcgis_count(query_url)` | TABLE: total |
| **L1** | `arcgis_ids(query_url)` | TABLE: oid_field, id_count |
| **L1** | `arcgis_extent(layer_url)` | TABLE: xmin..ymax, wkid, extent_geom |
| **L1** | `arcgis_fields(layer_url)` | TABLE: field_name, esri_type, duckdb_type, domain, ... |
| **L1** | `arcgis_domains(layer_url)` | TABLE: field_name, code, label |
| **L1** | `arcgis_subtypes(layer_url)` | TABLE: type_field, subtype_id, subtype_name, domains |
| **L1** | `arcgis_relationships(layer_url)` | TABLE: rel_id, rel_name, cardinality, key_field, ... |
| **L2** | `arcgis_check(url)` | TABLE: status, error_code, error_message, feature_count |
| **L2** | `arcgis_raw(url)` | TABLE: response as VARIANT (debugging) |
| **L2** | `arcgis_query(url)` | TABLE: properties + geometry (f=geojson, no CRS) |
| **L2** | `arcgis_read(url, crs)` | TABLE: properties + geometry WITH CRS (f=geojson). **crs must be VARCHAR** e.g. `'EPSG:4326'`, not integer |
| **L2** | `arcgis_read_json(url, crs)` | TABLE: attrs JSON + geometry (f=json fallback, ring-safe). **crs must be VARCHAR** |
| **L2** | `arcgis_stats(query_url)` | TABLE: attrs JSON (server-side statistics, f=json) |
| **L2** | `arcgis_query_extent(query_url)` | TABLE: xmin..ymax, feature_count, extent_geom |

## Common Workflows

```sql
-- Discover services and layers
SELECT * FROM arcgis_catalog('https://server/arcgis/rest/services?f=json');
SELECT * FROM arcgis_layers('https://server/.../FeatureServer?f=json');

-- Inspect a layer (VARIANT dot notation for deep exploration)
SELECT * FROM arcgis_meta('https://.../FeatureServer/0?f=json');
SELECT meta.name, meta.extent.spatialReference.latestWkid,
       meta.advancedQueryCapabilities.supportsPagination
FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json');

-- Quick recon: extent + count + IDs (no feature transfer)
SELECT * FROM arcgis_extent('https://.../FeatureServer/0?f=json');
SELECT * FROM arcgis_count('https://.../FeatureServer/0/query?where=1%3D1');
SELECT * FROM arcgis_ids('https://.../FeatureServer/0/query?where=1%3D1');

-- Download features with CRS (crs MUST be a string, e.g. 'EPSG:4326', never an integer)
SELECT * FROM arcgis_read(
    'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson',
    'EPSG:4326');

-- Export to GeoParquet (paginate first if count > maxRecordCount)
COPY (
    SELECT * FROM arcgis_read(
        'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson',
        'EPSG:4326')
) TO 'output.parquet' (FORMAT PARQUET);

-- Server-side statistics (no data transfer)
SELECT * FROM arcgis_stats(
    'https://.../FeatureServer/0/query?where=1%3D1'
    '&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22POP%22'
    '%2C%22outStatisticFieldName%22%3A%22total%22%7D%5D&f=json');

-- Paginated download (> maxRecordCount, with orderBy for reliability)
SELECT unnest(feature.properties),
       ST_SetCRS(ST_GeomFromGeoJSON(feature.geometry), 'EPSG:4326') AS geometry
FROM (
    SELECT unnest(features) AS feature
    FROM read_json_auto([
        'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
        '&returnGeometry=true&orderByFields=OBJECTID+ASC'
        '&resultOffset=' || x::VARCHAR || '&resultRecordCount=2000&f=geojson'
        FOR x IN range(0, 10000, 2000)
    ])
);

-- Paginated + proxy: wrap URL list with _arcgis_apply_proxy_list
SELECT unnest(feature.properties),
       ST_SetCRS(ST_GeomFromGeoJSON(feature.geometry), 'EPSG:4326') AS geometry
FROM (
    SELECT unnest(features) AS feature
    FROM read_json_auto(_arcgis_apply_proxy_list([
        'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
        '&returnGeometry=true&orderByFields=OBJECTID+ASC'
        '&resultOffset=' || x::VARCHAR || '&resultRecordCount=2000&f=geojson'
        FOR x IN range(0, 10000, 2000)
    ]))
);

-- Domain resolution (3 steps)
SET VARIABLE arcgis_layer = 'https://.../FeatureServer/0?f=json';
CREATE OR REPLACE TEMP TABLE _domains AS
WITH dl AS (SELECT * FROM arcgis_domains(getvariable('arcgis_layer')))
SELECT MAP(list(field_name), list(lookup)) AS all_domains
FROM (SELECT field_name, MAP(list(code::VARCHAR), list(label)) AS lookup FROM dl GROUP BY field_name);
CREATE OR REPLACE MACRO resolve_domain(field_val, field_name) AS
    COALESCE(
        (SELECT all_domains[field_name] FROM _domains)[field_val::VARCHAR],
        (SELECT all_domains[field_name] FROM _domains)[TRY_CAST(field_val AS INTEGER)::VARCHAR]
    );
```

## ArcGIS to GeoParquet (complete workflow)

Always follow these steps when downloading an ArcGIS layer to GeoParquet:

```sql
-- Step 1: Discover layers
SELECT * FROM arcgis_layers('https://server/.../MapServer?f=json');

-- Step 2: Check feature count (determines if pagination is needed)
SELECT * FROM arcgis_count('https://server/.../MapServer/0/query?where=1%3D1&f=json');

-- Step 3a: Small layer (count <= maxRecordCount, typically 1000-2000)
COPY (
    SELECT * FROM arcgis_read(
        'https://server/.../MapServer/0/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson',
        'EPSG:4326')
) TO 'output.parquet' (FORMAT PARQUET);

-- Step 3b: Large layer (count > maxRecordCount) — paginate with orderByFields
COPY (
    SELECT unnest(feature.properties),
           ST_SetCRS(ST_GeomFromGeoJSON(feature.geometry), 'EPSG:4326') AS geometry
    FROM (
        SELECT unnest(features) AS feature
        FROM read_json_auto([
            'https://server/.../MapServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
            '&returnGeometry=true&orderByFields=OBJECTID+ASC'
            '&resultOffset=' || x::VARCHAR || '&resultRecordCount=1000&f=geojson'
            FOR x IN range(0, <total_count + 1000>, 1000)
        ])
    )
) TO 'output.parquet' (FORMAT PARQUET);
```

**Critical rules:**
- CRS parameter MUST be a VARCHAR string like `'EPSG:4326'`, never an integer like `4326`
- Always check `arcgis_count()` first to decide between simple vs paginated download
- Use `orderByFields=OBJECTID+ASC` for reliable pagination (prevents duplicates/gaps)
- Set `resultRecordCount` to match the server's `maxRecordCount` (check via `arcgis_meta`)

## Reverse Proxy (proxy.ashx-style)

For ArcGIS servers behind a reverse proxy (proxy.ashx-style), where the proxy URL is prepended to the real URL:

```sql
-- Step 1: Set proxy prefix (trailing ? required)
SET VARIABLE arcgis_proxy = 'https://maps.example.com/proxy/proxy.ashx?';

-- Step 2: Set Referer header (scoped to proxy domain)
CREATE SECRET proxy_referer (TYPE HTTP,
    EXTRA_HTTP_HEADERS MAP {'Referer': 'https://maps.example.com/'},
    SCOPE 'https://maps.example.com/');

-- Step 3: Use macros normally, proxy is applied automatically
SELECT * FROM arcgis_layers(
    'https://gis-backend.example.com/server/rest/services/MyService/MapServer?f=json');

-- Disable proxy when switching to direct servers
SET VARIABLE arcgis_proxy = '';
```

## Authentication (11 methods)

```sql
-- Token as URL param (simplest)
SET VARIABLE arcgis_token = 'YOUR_TOKEN';

-- Bearer token (recommended for ArcGIS Online)
CREATE SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'YOUR_TOKEN');

-- X-Esri-Authorization (Enterprise with web-tier auth like IWA)
CREATE SECRET arcgis_auth (TYPE HTTP,
    EXTRA_HTTP_HEADERS MAP {'X-Esri-Authorization': 'Bearer YOUR_TOKEN'});

-- Scoped secrets (different tokens per server, auto-selected by URL)
CREATE SECRET agol (TYPE HTTP, BEARER_TOKEN 'online_token',
    SCOPE 'https://services1.arcgis.com/');
CREATE SECRET enterprise (TYPE HTTP, BEARER_TOKEN 'ent_token',
    SCOPE 'https://gis.mycompany.com/');

-- Bearer + Referer combo
CREATE SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'TOKEN',
    EXTRA_HTTP_HEADERS MAP {'Referer': 'https://myapp.example.com'});

-- Behind corporate proxy
CREATE SECRET arcgis_proxy (TYPE HTTP, BEARER_TOKEN 'TOKEN',
    HTTP_PROXY 'http://proxy:8080', HTTP_PROXY_USERNAME 'u', HTTP_PROXY_PASSWORD 'p');

-- Persistent (survives restarts)
CREATE PERSISTENT SECRET arcgis_auth (TYPE HTTP, BEARER_TOKEN 'TOKEN');

-- Generate token from Enterprise (requires http_client community ext)
INSTALL http_client FROM community; LOAD http_client;
SET VARIABLE arcgis_token = (
    SELECT ((http_post_form(
        'https://gis.example.com/portal/sharing/rest/generateToken',
        MAP {'username': 'u', 'password': 'p', 'client': 'referer',
             'referer': 'https://myapp.example.com', 'expiration': '60', 'f': 'json'},
        MAP {}
    )).body::JSON->>'token'));

-- OAuth client_credentials (requires http_client community ext)
CREATE SECRET arcgis_oauth (TYPE HTTP, BEARER_TOKEN (
    SELECT ((http_post_form('https://www.arcgis.com/sharing/rest/oauth2/token',
        MAP {'client_id': 'ID', 'client_secret': 'SECRET',
             'grant_type': 'client_credentials', 'f': 'json'}, MAP {}
    )).body::JSON->>'access_token')));

-- Verify secret selection: SELECT * FROM which_secret('https://...', 'http');
```

Not supported (interactive flows): IWA, PKI, SAML, OAuth authorization_code.
Workaround: generate token via browser/tool, then use any method above.

Full reference: `.claude/skills/duckdb/references/arcgis.sql`
