# Read Any Data File

Filename: `$0`
Question: `${1:-describe the data}`

## Step 1 -- Classify path

- **S3** (`s3://`) -> needs httpfs + S3 secret
- **HTTPS** (`https://`) -> needs httpfs
- **GCS** (`gs://`, `gcs://`) -> needs httpfs + GCS secret
- **Azure** (`azure://`, `az://`, `abfss://`) -> needs httpfs + Azure secret
- **Local** -> resolve with find

### Local file resolution
```bash
pixi run python -c "
import pathlib
matches = [p for p in pathlib.Path('.').rglob('$0') if '.git' not in p.parts]
for m in matches: print(m.resolve())
"
```
Zero results -> stop. Multiple -> ask user. One -> use as `RESOLVED_PATH`.

## Step 2 -- Set up state (remote files only)

Use the state resolution snippet from the main SKILL.md to find `STATE_DIR`.

For remote files, ensure credentials in state.sql:
- **S3**: `CREATE SECRET IF NOT EXISTS __default_s3 (TYPE S3, PROVIDER credential_chain);`
- **GCS** (two options):
  - **Option A** (HMAC keys via S3 API): `CREATE SECRET IF NOT EXISTS __default_gcs (TYPE GCS, PROVIDER credential_chain);`. Requires HMAC keys because DuckDB's httpfs uses the S3 API, not native GCP credentials.
  - **Option B** (native GCP): `INSTALL gcs FROM community; LOAD gcs;`. The community `duckdb-gcs` extension supports native GCP credentials directly.
- **Azure**: `CREATE SECRET IF NOT EXISTS __default_azure (TYPE AZURE, PROVIDER credential_chain, ACCOUNT_NAME '...');`
- **HTTPS**: just `LOAD httpfs;`

## Step 3 -- Ensure read_any macro

Check: `grep -q "read_any" "$STATE_DIR/state.sql"`

If missing, append the macro that dispatches to the right reader based on file extension:
- `.json/.jsonl/.geojson` -> `read_json_auto`
- `.csv/.tsv/.txt` -> `read_csv`
- `.parquet/.pq` -> `read_parquet`
- `.avro` -> `read_avro`
- `.xlsx/.xls` -> `read_xlsx` (needs spatial ext)
- `.shp/.gpkg/.fgb/.kml` -> `st_read` (needs spatial ext)
- `.ipynb` -> `read_json_auto` + unnest cells
- `.db/.sqlite` -> `sqlite_scan` (needs sqlite_scanner ext)

## Step 4 -- Read the file

**Local** (sandboxed):
```bash
pixi run duckdb -init "$STATE_DIR/state.sql" -csv -c "
SET allowed_paths=['RESOLVED_PATH'];
SET enable_external_access=false;
SET allow_persistent_secrets=false;
SET lock_configuration=true;
DESCRIBE FROM read_any('RESOLVED_PATH');
SELECT count(*) AS row_count FROM read_any('RESOLVED_PATH');
FROM read_any('RESOLVED_PATH') LIMIT 10;
"
```

**Remote**: same queries but without sandbox settings, use state.sql for secrets.

**Spatial files**: add stem-wildcard to allowed_paths for sidecar files:
`SET allowed_paths=['RESOLVED_PATH', 'RESOLVED_PATH_WITHOUT_EXTENSION.*']`

On failure: install missing extensions via [install.md](install.md), fix reader, or search [docs-search.md](docs-search.md).

## Step 5 -- Answer

Using schema + samples, answer: `${1:-describe the data}`.
Suggest follow-up queries via [query.md](query.md) or [attach-db.md](attach-db.md) for large files.
For ArcGIS FeatureServer URLs, use [arcgis.md](arcgis.md) macros instead.
