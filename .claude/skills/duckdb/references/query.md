# Query Execution

Input: `$@`

## Step 1 -- Determine mode

Use the state resolution snippet from the main SKILL.md to find `STATE_DIR`.

**Mode**: Ad-hoc if `--file` flag present, SQL references file paths, or no state. Session if state exists and input references tables/is natural language.

## Step 2 -- Generate SQL if needed

For natural language, first get schema context (session mode):
```bash
pixi run duckdb -init "$STATE_DIR/state.sql" -csv -c "SELECT table_name FROM duckdb_tables() ORDER BY table_name;"
pixi run duckdb -init "$STATE_DIR/state.sql" -csv -c "DESCRIBE <table_name>;"
```

## Step 3 -- Estimate result size

Skip for DESCRIBE, SUMMARIZE, aggregations, or queries with LIMIT.
For unbounded queries on >1M rows: suggest adding LIMIT or aggregation before running.

## Step 4 -- Execute

**Ad-hoc** (sandboxed):
```bash
pixi run duckdb :memory: -csv <<'SQL'
SET allowed_paths=['FILE_PATH'];
SET enable_external_access=false;
SET allow_persistent_secrets=false;
SET lock_configuration=true;
<QUERY>;
SQL
```

**Session**:
```bash
pixi run duckdb -init "$STATE_DIR/state.sql" -csv <<'SQL'
<QUERY>;
SQL
```

## Step 5 -- Handle errors

- Syntax error -> suggest fix, re-run
- Missing extension -> delegate to [install.md](install.md), retry
- Table not found -> list tables with `FROM duckdb_tables()`
- File not found -> search with `find "$PWD" -name "<filename>"`
- Unclear error -> search with [docs-search.md](docs-search.md)
