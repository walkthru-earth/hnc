# DuckLake - Open Data Lake Format

DuckLake is an open data lake and catalog format from the DuckDB team. Stores data as Parquet files, metadata in a SQL database (DuckDB, PostgreSQL, SQLite, MySQL).

## Install

```sql
INSTALL ducklake;
LOAD ducklake;
```

Requires DuckDB v1.3.0+. Current spec version: **1.0** (DuckDB 1.5.2).

## Choosing a catalog backend

| Backend | Multi-client | Setup | Best for | Limitations |
|---------|-------------|-------|----------|-------------|
| **DuckDB** | No, single-client only | Zero config, just a file path | Local dev, prototyping, single-user workflows | Cannot share across processes or machines |
| **PostgreSQL** | Yes, full concurrent access | Requires running PG 12+ server | Production, team collaboration, CI/CD pipelines | Heavier infra, needs `postgres` extension |
| **SQLite** | Limited, no concurrent read+write | Just a file path, lightweight | Embedded apps, edge deployments, simple sharing | No simultaneous readers and writers |
| **MySQL** | Yes (in theory) | Requires running MySQL 8+ server | Environments already running MySQL | Known issues, not recommended by DuckDB team |

**Rule of thumb**: Start with DuckDB for local work. Move to PostgreSQL when you need multiple clients or production durability.

**CRITICAL for this project**: We use the DuckDB catalog backend (.duckdb), NOT SQLite (.ducklake). DuckDB catalogs support remote S3/HTTPS read-only access via httpfs (`ATTACH 'ducklake:s3://bucket/{owner}/{repo}/{branch}/catalog.duckdb' AS cat (READ_ONLY)`). SQLite catalogs do NOT support remote access (blocked by duckdb/ducklake#912).

## Attach a DuckLake catalog

```sql
-- DuckDB catalog (single-client only)
ATTACH 'ducklake:metadata.ducklake' AS my_lake;

-- PostgreSQL catalog (multi-client, requires postgres extension)
INSTALL postgres;
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS my_lake
    (DATA_PATH 'data_files/');

-- SQLite catalog (requires sqlite extension)
INSTALL sqlite;
ATTACH 'ducklake:sqlite:metadata.sqlite' AS my_lake
    (DATA_PATH 'data_files/');

-- Cloud storage for data files
ATTACH 'ducklake:metadata.ducklake' AS my_lake
    (DATA_PATH 's3://my-bucket/lake/');
```

### ATTACH options (verified DuckDB 1.5.2 / spec 1.0)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DATA_PATH` | `<metadata_file>.files` | Storage location for Parquet data files |
| `READ_ONLY` | false | Read-only mode |
| `AUTOMATIC_MIGRATION` | false | Auto-migrate catalog on spec version mismatch |
| `OVERRIDE_DATA_PATH` | true | Override stored DATA_PATH for current session |
| `SNAPSHOT_VERSION` | (none) | Connect at specific snapshot ID |
| `SNAPSHOT_TIME` | (none) | Connect at specific timestamp |
| `CREATE_IF_NOT_EXISTS` | true | Create DuckLake if not exists |
| `DATA_INLINING_ROW_LIMIT` | 0 | Rows for data inlining at creation time |
| `ENCRYPTED` | false | Enable data encryption |
| `METADATA_CATALOG` | auto | Attached catalog database name |
| `METADATA_SCHEMA` | main | Schema for DuckLake metadata tables |
| `METADATA_PATH` | (from connect string) | Connection string to metadata catalog |

**IMPORTANT**: `MIGRATE_IF_REQUIRED` is NOT supported in DuckLake. Use `AUTOMATIC_MIGRATION` instead. In DuckLake 1.0, attaching a catalog with a version mismatch throws an error unless `AUTOMATIC_MIGRATION` is set to true.

## Remote S3 Access (DuckLake + httpfs)

DuckLake remote attach requires `CREATE SECRET` for S3 credentials. The `SET s3_*` variables work for direct Parquet/httpfs reads but do NOT work for DuckLake catalog attachment. This is because DuckLake opens the catalog .duckdb file through a separate connection that only resolves credentials via the secret manager.

```sql
INSTALL ducklake; LOAD ducklake;
INSTALL httpfs;   LOAD httpfs;

-- REQUIRED: CREATE SECRET (SET s3_* variables will NOT work for DuckLake)
CREATE SECRET __default_s3 (
    TYPE S3,
    KEY_ID 'your-access-key',
    SECRET 'your-secret-key',
    ENDPOINT 'fsn1.your-objectstorage.com',
    REGION 'fsn1',
    URL_STYLE 'path'
);

-- Remote read-only attach (metadata queries, snapshots, file listings)
-- Paths include {owner}/{repo}/{branch}/ prefix for repo/branch isolation
ATTACH 'ducklake:s3://bucket/{owner}/{repo}/{branch}/catalog.duckdb' AS global (READ_ONLY);

-- Query catalog metadata
SELECT * FROM ducklake_snapshots('global');
USE global."opensky-flights";
SELECT * FROM ducklake_list_files('global', 'states');

-- Query data through the catalog
SELECT COUNT(*) FROM global."opensky-flights".states;
SELECT * FROM global."test-minimal".data LIMIT 5;

-- Remote read-write (for CI merge scripts, uses the global catalog)
ATTACH 'ducklake:s3://bucket/{owner}/{repo}/{branch}/catalog.duckdb' AS global_cat
    (DATA_PATH 's3://bucket/{owner}/{repo}/{branch}/');
```

**When to use `SET s3_*` vs `CREATE SECRET`:**

| Operation | `SET s3_*` | `CREATE SECRET` |
|-----------|-----------|-----------------|
| `FROM 's3://bucket/file.parquet'` | Works | Works |
| `glob('s3://bucket/*')` | Works | Works |
| `ATTACH 'ducklake:s3://...'` | Does NOT work | Required |
| DuckLake data reads through catalog | Does NOT work | Required |

**Tip:** When using both DuckLake and direct Parquet reads in the same session, just use `CREATE SECRET`. It covers all cases.

## Basic operations

```sql
USE my_lake;
CREATE TABLE tbl (id INTEGER, name VARCHAR);
INSERT INTO tbl VALUES (1, 'hello');
SELECT * FROM tbl;
UPDATE tbl SET name = 'world' WHERE id = 1;
```

## DuckLake functions (verified DuckDB 1.5.2)

### ducklake_add_data_files

Registers existing Parquet files with DuckLake without copying them. Once added, **DuckLake assumes ownership** and compaction operations can cause the files to be deleted.

```sql
CALL ducklake_add_data_files('catalog', 'table', 'file.parquet');
CALL ducklake_add_data_files('catalog', 'table', 'file.parquet',
    schema => 'my_schema');
CALL ducklake_add_data_files('catalog', 'table', 'file.parquet',
    allow_missing => true);         -- missing table columns get default values
CALL ducklake_add_data_files('catalog', 'table', 'file.parquet',
    ignore_extra_columns => true);  -- extra file columns are ignored
```

**Parameters:**
- `catalog` (VARCHAR): attached DuckLake catalog name
- `table` (VARCHAR): target table name
- `file_path` (VARCHAR): path to Parquet file (single file only)
- `schema` (named, VARCHAR): target schema, default 'main'
- `allow_missing` (named, BOOLEAN): substitute defaults for missing columns
- `ignore_extra_columns` (named, BOOLEAN): ignore extra columns in file

**Known issues:**
- duckdb/ducklake#579: Hive partition metadata not populated by add_data_files
- duckdb/ducklake#898: Can be very slow on large catalogs (fixed)

### ducklake_list_files

Returns data files and delete files for a table.

```sql
SELECT * FROM ducklake_list_files('catalog', 'table');
SELECT * FROM ducklake_list_files('catalog', 'table', schema => 'my_schema');
SELECT * FROM ducklake_list_files('catalog', 'table', snapshot_version => 2);
SELECT * FROM ducklake_list_files('catalog', 'table',
    snapshot_time => '2025-06-16 15:24:30');
```

**Return columns:** `data_file`, `data_file_size_bytes`, `data_file_footer_size`, `data_file_encryption_key`, `delete_file`, `delete_file_size_bytes`, `delete_file_footer_size`, `delete_file_encryption_key`

### ducklake_snapshots

```sql
SELECT * FROM ducklake_snapshots('catalog');
-- Returns: snapshot_id, snapshot_time, schema_version, changes, author, commit_message, commit_extra_info
```

### ducklake_table_info

```sql
SELECT * FROM ducklake_table_info('catalog');
-- Returns: table_name, schema_id, table_id, table_uuid, file_count, file_size_bytes, delete_file_count, delete_file_size_bytes
```

### ducklake_settings (new in 1.0)

```sql
SELECT * FROM ducklake_settings('catalog');
-- Or: FROM my_lake.settings();
-- Returns: dbms_type, extension_version, data_path
```

### ducklake_flush_inlined_data (new in 1.0)

Materializes inlined data to Parquet files.

```sql
CALL ducklake_flush_inlined_data('catalog');
CALL ducklake_flush_inlined_data('catalog', schema_name => 'my_schema');
CALL ducklake_flush_inlined_data('catalog', schema_name => 'my_schema', table_name => 'tbl');
-- Returns: schema_name, table_name, rows_flushed
```

### ducklake_rewrite_data_files (new in 1.0)

Rewrites files with many deletions (default threshold: 95% deleted).

```sql
CALL ducklake_rewrite_data_files('catalog');
CALL ducklake_rewrite_data_files('catalog', delete_threshold => 0.5);
-- Returns: schema_name, table_name, files_processed, files_created
```

### ducklake_table_insertions / ducklake_table_deletions / ducklake_table_changes

```sql
SELECT * FROM ducklake_table_insertions('catalog', 'schema', 'table', start_snap, end_snap);
SELECT * FROM ducklake_table_deletions('catalog', 'schema', 'table', start_snap, end_snap);
SELECT * FROM ducklake_table_changes('catalog', 'schema', 'table', start_snap, end_snap);
-- Returns rows with snapshot_id, rowid, change_type (insert/delete), plus original columns
```

## Configuration (set_option)

Set options at global, schema, or table scope. Priority: table > schema > global.

```sql
-- Global scope
CALL my_lake.set_option('parquet_compression', 'zstd');

-- Schema scope
CALL my_lake.set_option('auto_compact', false, schema => 'my_schema');

-- Table scope
CALL my_lake.set_option('auto_compact', false, schema => 'my_schema', table_name => 'tbl');

-- Query all options
FROM my_lake.options();
```

### Available options (verified DuckDB 1.5.2)

| Option | Type | Default | Scopes | Description |
|--------|------|---------|--------|-------------|
| `auto_compact` | boolean | true | G/S/T | Include in CHECKPOINT compaction |
| `expire_older_than` | duration | (unset) | G | Default threshold for ducklake_expire_snapshots |
| `delete_older_than` | duration | (unset) | G | Threshold for cleanup/orphan deletion |
| `hive_file_pattern` | boolean | true* | G | Write Hive-style partition paths |
| `parquet_compression` | string | snappy | G/S/T | snappy, zstd, gzip, lz4, brotli, etc. |
| `parquet_version` | integer | 1 | G/S/T | Parquet format version (1 or 2) |
| `parquet_compression_level` | integer | 3 | G/S/T | Compression level |
| `parquet_row_group_size` | integer | 122880 | G/S/T | Rows per row group |
| `parquet_row_group_size_bytes` | size | (unset) | G/S/T | Byte limit per row group |
| `target_file_size` | size | 512MB | G/S/T | Target file size for compaction |
| `require_commit_message` | boolean | false | G | Require commit message on writes |
| `rewrite_delete_threshold` | float | 0.95 | G/S/T | Min deleted fraction before rewrite |
| `per_thread_output` | boolean | false | G/S/T | Separate output files per thread |
| `data_inlining_row_limit` | integer | 10** | G/S/T | Max rows to inline in metadata |
| `encrypted` | boolean | false | G/S/T | Encrypt Parquet files |

*hive_file_pattern does not appear in options() until explicitly set, but the effective default is true (Hive-style paths are used for partitioned data).

**Extension-level default `ducklake_default_data_inlining_row_limit` = 10. Small INSERTs (<= 10 rows) are inlined in catalog metadata, not written as Parquet files.

**GOTCHA: `hive_file_pattern` and `require_commit_message` require boolean values, NOT strings.** `set_option('hive_file_pattern', 'false')` will FAIL with "Could not convert string 'false' to INT8". Use `set_option('hive_file_pattern', false)` instead.

## Maintenance

### CHECKPOINT (all-in-one)

Runs all 6 maintenance steps in sequence:
1. `ducklake_flush_inlined_data`
2. `ducklake_expire_snapshots`
3. `ducklake_merge_adjacent_files`
4. `ducklake_rewrite_data_files`
5. `ducklake_cleanup_old_files`
6. `ducklake_delete_orphaned_files`

```sql
USE my_lake;
CHECKPOINT;
```

Respects `auto_compact` setting. If `auto_compact = false`, steps 1/3/4/6 are skipped (only expire_snapshots and cleanup_old_files run).

### Individual maintenance functions

```sql
-- Expire snapshots older than threshold
CALL ducklake_expire_snapshots('catalog', older_than => now() - INTERVAL '30 days');
CALL ducklake_expire_snapshots('catalog', versions => [2, 3]);  -- specific versions
CALL ducklake_expire_snapshots('catalog', dry_run => true, older_than => now() - INTERVAL '1 week');

-- Merge small adjacent files
CALL ducklake_merge_adjacent_files('catalog');
-- Returns: schema_name, table_name, files_processed, files_created

-- Rewrite files with many deletions
CALL ducklake_rewrite_data_files('catalog');

-- Clean up files from expired snapshots
CALL ducklake_cleanup_old_files('catalog', cleanup_all => true);
CALL ducklake_cleanup_old_files('catalog', older_than => now() - INTERVAL '1 week');
CALL ducklake_cleanup_old_files('catalog', dry_run => true);

-- Clean up orphaned files (from crashed writes)
CALL ducklake_delete_orphaned_files('catalog', cleanup_all => true);
CALL ducklake_delete_orphaned_files('catalog', older_than => now() - INTERVAL '7 days');
CALL ducklake_delete_orphaned_files('catalog', dry_run => true);

-- Flush inlined data to Parquet files
CALL ducklake_flush_inlined_data('catalog');
```

**NOTE:** `ducklake_delete_orphaned_files` is already called as step 6 of CHECKPOINT. Calling it separately after CHECKPOINT is redundant.

## Time travel

```sql
-- By version number
SELECT * FROM tbl AT (VERSION => 3);

-- By timestamp
SELECT * FROM tbl AT (TIMESTAMP => now() - INTERVAL '1 week');

-- Attach entire DB at a specific snapshot
ATTACH 'ducklake:metadata.duckdb' (SNAPSHOT_VERSION 3);
ATTACH 'ducklake:metadata.duckdb' (SNAPSHOT_TIME '2025-05-26 00:00:00');

-- List all snapshots
SELECT * FROM ducklake_snapshots('my_lake');
```

## Schema evolution

```sql
ALTER TABLE tbl ADD COLUMN new_col INTEGER;
ALTER TABLE tbl ADD COLUMN new_col VARCHAR DEFAULT 'my_default';
ALTER TABLE tbl DROP COLUMN old_col;
ALTER TABLE tbl RENAME old_col TO new_col;
ALTER TABLE tbl ALTER col1 SET TYPE BIGINT;
-- Nested struct fields
ALTER TABLE tbl ADD COLUMN nested.new_field INTEGER;
```

## Partitioning

```sql
-- Partition by column (Hive-style by default)
ALTER TABLE tbl SET PARTITIONED BY (region);

-- Temporal transforms: year, month, day, hour, identity
ALTER TABLE tbl SET PARTITIONED BY (year(created_at));
ALTER TABLE tbl SET PARTITIONED BY (year(ts), month(ts));

-- Remove partitioning (only affects new writes)
ALTER TABLE tbl RESET PARTITIONED BY;
```

Supported transforms: `identity`, `year`, `month`, `day`, `hour`, `bucket(N, col)`.

```sql
-- Bucket partitioning (Iceberg-compatible, murmur3 hash)
ALTER TABLE tbl SET PARTITIONED BY (bucket(16, id));
-- Combine transforms
ALTER TABLE tbl SET PARTITIONED BY (year(ts), bucket(8, region));
```

Partition layout changes are evolutionary. Only new data uses the updated scheme. Existing data keeps its original layout.

## Key features

- **Snapshots**: every write creates a snapshot, queryable via `ducklake_snapshots()`
- **ACID transactions**: multi-table transactional guarantees via the catalog DB
- **Multi-client**: multiple DuckDB instances share one dataset via PostgreSQL/SQLite catalog
- **Cloud storage**: DATA_PATH supports S3, GCS, Azure, R2, NFS
- **Geometry support**: spatial columns stored natively (v0.3+)
- **Data inlining**: sub-millisecond writes for small data (default: <= 10 rows)
- **Partitioning**: Hive-style with temporal transforms, bucket partitioning (v1.0)
- **Sorted tables**: expression-based sort keys with automatic sorting during compaction (v1.0)
- **Deletion vectors**: experimental Iceberg v3 compatible roaring bitmaps (v1.0)
- **Data change feed**: track insertions/deletions/changes between snapshots

## Limitations

- No indexes, primary keys, foreign keys, UNIQUE, or CHECK constraints
- Parquet-only storage (no .duckdb files as data files)
- DuckDB catalog backend is single-client only
- `ducklake_add_data_files` accepts only single files (not globs)
- `hive_file_pattern` / `require_commit_message` must be set with boolean values, not strings

## Shared-ownership compaction danger

**Verified 2026-04-13 with DuckDB 1.5.2 / DuckLake spec 1.0.**

> **Note:** This project uses a single global catalog, so shared ownership does not apply. This section documents a general DuckLake pitfall for reference.

When using `ducklake_add_data_files` to register the same Parquet files in multiple catalogs, compaction on one catalog can **delete files still referenced by the other**.

**Root cause:** `ducklake_add_data_files` transfers ownership. DuckLake tracks files for lifecycle management. When compaction merges files, the originals are scheduled for deletion. `cleanup_old_files` then deletes them from storage. Other catalogs referencing those files have no knowledge of this.

**Mitigation:** Either use a single catalog (as this project does), or set `auto_compact = false` on any catalog whose files are shared with another.

Test script: `.github/scripts/test_ducklake_api.py` (TEST 7 demonstrates this).

## Pitfall: Never Overwrite Registered Files

`ducklake_add_data_files()` records `file_size_bytes` and `footer_size` at registration time. DuckLake uses these cached values for range requests when reading. If the file is later overwritten at the same S3 path (e.g., by a re-extraction), the stored metadata becomes stale. DuckLake will request a byte range past the end of the smaller file, causing **HTTP 416 Range Not Satisfiable** on S3.

**Symptoms:** `ATTACH` succeeds, metadata queries work (`ducklake_snapshots`, `ducklake_list_files`), but `SELECT * FROM table` fails with HTTP 416. Direct `read_parquet('s3://...')` works fine because it issues a fresh HEAD request.

**Diagnosis:**
```sql
-- Compare stored size vs actual
-- Stored (from catalog):
SELECT path, file_size_bytes, footer_size FROM ducklake_data_file WHERE end_snapshot IS NULL;
-- Actual: curl -I on the S3 URL, check Content-Length
```

**Prevention:** Use unique file paths per extraction (e.g., timestamped filenames or DuckLake-managed writes via INSERT). Never overwrite a registered Parquet file at the same path.

## Detach

```sql
USE memory;
DETACH my_lake;
```

## Testing

Run the comprehensive API test locally (no S3 needed):

```bash
uv run .github/scripts/test_ducklake_api.py
```

This tests all DuckLake APIs used by the registry: ATTACH options, set_option, ducklake_add_data_files, ducklake_list_files, merge workflow, compaction safety, maintenance functions, partitioning, time travel, and more.

## Documentation

Search DuckLake docs via [docs-search.md](docs-search.md) using the DuckLake index at `https://ducklake.select/data/docs-search.duckdb`.
Full docs: https://ducklake.select
