"""Inference-result cache, mirrors cache.py shard layout.

Each TRIBE v2 inference takes ~100 s of GPU time per image. Persisting
the parcel-level brain_activity vector and top_regions per image_id
lets a re-run skip the whole encode+predict path on a cache hit, and
makes mid-batch crashes recoverable.

Shards: cache_inference/cache_part_<ISO8601>.parquet
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

CACHE_DIR: Path = Path("cache_inference")
SENTINEL_NAME = "cache_part_000000_init.parquet"

_SCHEMA_SQL = """
    image_id        VARCHAR,
    brain_activity  FLOAT[],
    top_regions     STRUCT(name VARCHAR, score FLOAT)[],
    inferred_at     TIMESTAMP,
    model_repo      VARCHAR
"""

_COLUMNS = ["image_id", "brain_activity", "top_regions", "inferred_at", "model_repo"]


@dataclass
class InferenceRow:
    image_id: str
    brain_activity: list[float]
    top_regions: list[dict[str, Any]]
    inferred_at: datetime
    model_repo: str


def _create_staging(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE inference_staging ({_SCHEMA_SQL})")


def ensure_sentinel(cache_dir: Path = CACHE_DIR) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    sentinel = cache_dir / SENTINEL_NAME
    if sentinel.exists():
        return
    con = duckdb.connect(":memory:")
    try:
        _create_staging(con)
        con.execute(
            "INSERT INTO inference_staging VALUES (?, ?, ?, ?, ?)",
            [
                "__sentinel__",
                [],
                [],
                datetime.now(UTC).replace(tzinfo=None),
                "",
            ],
        )
        tmp = sentinel.with_suffix(sentinel.suffix + ".tmp")
        con.execute(
            f"COPY (SELECT * FROM inference_staging) TO '{tmp.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9, ROW_GROUP_SIZE 1024)"
        )
        os.replace(tmp, sentinel)
    finally:
        con.close()


def cached_ids(con: duckdb.DuckDBPyConnection, cache_dir: Path = CACHE_DIR) -> set[str]:
    ensure_sentinel(cache_dir)
    glob = (cache_dir / "cache_part_*.parquet").as_posix()
    rows = con.execute(
        f"SELECT image_id FROM read_parquet('{glob}', union_by_name=true) "
        "WHERE image_id <> '__sentinel__'"
    ).fetchall()
    return {r[0] for r in rows}


def filter_new(candidate_ids: Iterable[str], cache_dir: Path = CACHE_DIR) -> list[str]:
    candidates = list(candidate_ids)
    con = duckdb.connect(":memory:")
    try:
        existing = cached_ids(con, cache_dir)
    finally:
        con.close()
    return [cid for cid in candidates if cid not in existing]


def write_shard(rows: Sequence[InferenceRow], cache_dir: Path = CACHE_DIR) -> Path | None:
    if not rows:
        return None
    cache_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    final = cache_dir / f"cache_part_{ts}.parquet"
    tmp = final.with_suffix(final.suffix + ".tmp")
    con = duckdb.connect(":memory:")
    try:
        _create_staging(con)
        con.executemany(
            "INSERT INTO inference_staging VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r.image_id,
                    list(r.brain_activity),
                    list(r.top_regions),
                    r.inferred_at,
                    r.model_repo,
                )
                for r in rows
            ],
        )
        con.execute(
            f"COPY (SELECT * FROM inference_staging) TO '{tmp.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9, ROW_GROUP_SIZE 1024)"
        )
        os.replace(tmp, final)
    finally:
        con.close()
    return final


def load_into_table(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    image_ids: Sequence[str],
    cache_dir: Path = CACHE_DIR,
) -> int:
    if not image_ids:
        return 0
    ensure_sentinel(cache_dir)
    glob = (cache_dir / "cache_part_*.parquet").as_posix()
    placeholders = ", ".join(["?"] * len(image_ids))
    sql = f"""
        INSERT INTO {table} (image_id, brain_activity, top_regions)
        SELECT image_id, brain_activity, top_regions
        FROM read_parquet('{glob}', union_by_name=true)
        WHERE image_id <> '__sentinel__' AND image_id IN ({placeholders})
    """
    con.execute(sql, list(image_ids))
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(n[0]) if n else 0
