# Search Past Session Logs

Search silently, do NOT narrate to the user.

`$0` = keyword. `$1` = `--here` to scope to current project.

## Search path

```bash
ALL="$HOME/.claude/projects/*/*.jsonl"
CURRENT="$HOME/.claude/projects/$(pixi run python -c "import pathlib,re; print(re.sub(r'[/\\\\_]', '-', str(pathlib.Path.cwd())))")/*.jsonl"
```

Use `$CURRENT` if `--here`, else `$ALL`.

## Query

```bash
pixi run duckdb :memory: -c "
SELECT
  regexp_extract(filename, 'projects/([^/]+)/', 1) AS project,
  strftime(timestamp::TIMESTAMPTZ, '%Y-%m-%d %H:%M') AS ts,
  message.role, message.content::VARCHAR AS content
FROM read_ndjson('<PATH>', auto_detect=true, ignore_errors=true, filename=true)
WHERE message::VARCHAR ILIKE '%<KEYWORD>%' AND message.role IS NOT NULL
ORDER BY timestamp LIMIT 40;
"
```

## Large results

If >40 rows, offload to a temp DuckDB file to avoid flooding context:
```bash
pixi run duckdb ".claude/skills/duckdb/references/memories.duckdb" -c "CREATE OR REPLACE TABLE memories AS <above query without LIMIT>;"
```

Then drill down interactively:
```bash
pixi run duckdb ".claude/skills/duckdb/references/memories.duckdb" -c "
  SELECT DISTINCT project FROM memories;
  SELECT ts, role, left(content, 200) FROM memories WHERE content ILIKE '%<refined_keyword>%' ORDER BY ts DESC LIMIT 20;
"
```

Clean up when done: `rm -f .claude/skills/duckdb/references/memories.duckdb`

## Internalize

Extract decisions, patterns, conventions, unresolved items. Use to inform current response.
