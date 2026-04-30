# Install or Update Extensions

Arguments: `$@`

Parse each extension as `name` -> `INSTALL name;` or `name@repo` -> `INSTALL name FROM repo;`.

## Step 1 -- Locate DuckDB

```bash
pixi run duckdb --version
```

If pixi fails, fall back to system `duckdb --version`. If neither works, tell the user to install via `pixi add duckdb` or one of:
- macOS: `brew install duckdb`
- Linux: `curl -fsSL https://install.duckdb.org | sh`
- Windows: `winget install DuckDB.cli`

## Step 2 -- Check CLI version (update mode only)

```bash
CURRENT=$(pixi run duckdb --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
LATEST=$(curl -fsSL https://duckdb.org/data/latest_stable_version.txt 2>/dev/null)
```

If `$CURRENT` != `$LATEST`, suggest upgrading:
- pixi: `pixi add duckdb-cli` (or update version constraint)
- brew: `brew upgrade duckdb`
- manual: https://duckdb.org/docs/installation

## Step 3 -- Install or update

**Install mode** (no `--update` flag):

```bash
pixi run duckdb :memory: -c "INSTALL ext1; INSTALL ext2 FROM repo2; ..."
```

**Update mode** (`--update` in `$@`):

```bash
pixi run duckdb :memory: -c "UPDATE EXTENSIONS;"
# Or specific: UPDATE EXTENSIONS (ext1, ext2);
```

Report success or failure. Common extensions for this project: `spatial`, `httpfs`, `fts`, `parquet`.
