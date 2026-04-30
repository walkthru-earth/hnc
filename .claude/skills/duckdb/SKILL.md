---
name: duckdb
description: >
  DuckDB v1.5 spatial/GIS analytics with ST_*, H3, A5, and S2/geography extensions.
  Use for SQL queries, file exploration, spatial analysis, discrete global grids,
  GeoParquet, ArcGIS REST, DuckLake, session state, docs search, or extension
  management, even without saying "DuckDB".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
---

Run via `pixi run duckdb`. All tools through pixi.

## Routing

Read the relevant reference when working on a specific task.

| Reference | When to load |
|-----------|-------------|
| [query.md](references/query.md) | Running SQL (raw or natural language), session vs ad-hoc mode, error handling |
| [read-file.md](references/read-file.md) | Exploring any data file (CSV, Parquet, Excel, JSON, Avro, spatial, remote S3/GCS/Azure) |
| [attach-db.md](references/attach-db.md) | Attaching a .duckdb database for persistent querying |
| [state.md](references/state.md) | Initializing/managing state.sql (extensions, credentials, macros, atomic updates) |
| [docs-search.md](references/docs-search.md) | Searching DuckDB/DuckLake documentation via FTS |
| [install.md](references/install.md) | Installing or updating DuckDB extensions |
| [read-memories.md](references/read-memories.md) | Recovering context from past Claude Code sessions |
| [spatial.md](references/spatial.md) | ST_* spatial functions, geometry ops, CRS transforms, spatial joins |
| [arcgis.md](references/arcgis.md) | ArcGIS REST macros (19 macros), pagination, proxy, authentication |
| [ducklake.md](references/ducklake.md) | DuckLake open data lake format, time travel, snapshots, schema evolution, multi-client |
| [h3.md](references/h3.md) | H3 hexagonal discrete global grid system (70+ functions), cell indexing, traversal, polygon conversion |
| [a5.md](references/a5.md) | A5 pentagonal equal-area geospatial index (31 resolutions), millimeter-accurate, traversal, compaction |
| [geography.md](references/geography.md) | S2 spherical geometry (s2_* functions), geodesic edges, S2 cell indexing, coverings, BigQuery-compatible |

## State resolution (shared by query, read-file, attach-db)

```bash
STATE_DIR=""
SKILL_DIR=".claude/skills/duckdb/references"
test -f "$SKILL_DIR/state.sql" && STATE_DIR="$SKILL_DIR"
```

**Mode**: Ad-hoc if `--file` flag present, SQL references file paths, or no state. Session if state exists and input references tables/is natural language.

## DuckDB Friendly SQL Reference

### Compact clauses
- `FROM table WHERE x > 10` -- implicit SELECT *
- `GROUP BY ALL` / `ORDER BY ALL` -- auto-detect columns
- `SELECT * EXCLUDE (col1, col2)` / `REPLACE (expr AS col)`
- `UNION ALL BY NAME` -- combine tables with different column orders
- `LIMIT 10%` -- percentage limit
- `SELECT x: 42` -- prefix alias syntax
- Trailing commas allowed in SELECT

### Query features
- `count()` instead of `count(*)`
- Reusable aliases in WHERE/GROUP BY/HAVING
- Lateral column aliases: `SELECT i+1 AS j, j+2 AS k`
- `COLUMNS(*)` with regex, EXCLUDE, REPLACE, lambdas
- `FILTER (WHERE ...)` for conditional aggregation
- GROUPING SETS / CUBE / ROLLUP
- `max(col, 3)` -- top-N as list; `arg_max(arg, val, n)`, `min_by(arg, val, n)`
- DESCRIBE, SUMMARIZE, PIVOT / UNPIVOT
- `SET VARIABLE x = expr` -> `getvariable('x')`

### Data import
- Direct: `FROM 'file.csv'`, `FROM 'data.parquet'`
- Globbing: `FROM 'data/part-*.parquet'`
- Auto-detection for CSV headers/schemas

### Expressions
- Dot chaining: `'hello'.upper()`, `col.trim().lower()`
- List comprehensions: `[x*2 FOR x IN list_col]`
- Slicing: `col[1:3]`, `col[-1]`
- STRUCT: `SELECT s.* FROM (SELECT {'a': 1} AS s)`
- `format('{}->{}', a, b)`

### Joins
- ASOF joins -- approximate matching on ordered data
- POSITIONAL joins -- match by position
- LATERAL joins -- reference prior expressions

### DDL
- `CREATE OR REPLACE TABLE` -- no DROP needed
- CTAS: `CREATE TABLE ... AS SELECT`
- `INSERT INTO ... BY NAME` -- match by column name
- `INSERT OR IGNORE INTO` / `INSERT OR REPLACE INTO`

## Cross-references
- **gdal** skill -- complete unified GDAL CLI reference, Esri driver references in `references/esri-*.md`
- **geoparquet** skill -- gpio for Hilbert sorting, bbox covering, validation
- **data-pipeline** skill -- multi-step ETL workflows as pixi tasks
- **data-explorer** agent -- proactive dataset profiling (DuckDB + GDAL + gpio)
- **data-quality** agent -- deep validation (nulls, geometry, CRS consistency)
- **pipeline-orchestrator** agent -- multi-step workflow generation with pixi tasks
