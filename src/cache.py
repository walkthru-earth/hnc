from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from dataclasses import fields as dc_fields
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from .mapillary_client import ImageMeta

CACHE_DIR: Path = Path("cache_images")
SENTINEL_NAME = "cache_part_000000_init.parquet"

# Column order matches PLAN section 4 cache schema verbatim.
_SCHEMA_SQL = """
    image_id              VARCHAR,
    image_blob            BLOB,
    image_sha256          VARCHAR,
    image_mime            VARCHAR,
    image_bytes           BIGINT,
    image_width           INTEGER,
    image_height          INTEGER,
    downloaded_at         TIMESTAMP,
    source                VARCHAR,
    mapillary_bbox_query  VARCHAR,
    lon                   DOUBLE,
    lat                   DOUBLE,
    compass_angle         DOUBLE,
    captured_at           TIMESTAMP,
    camera_type           VARCHAR,
    is_pano               BOOLEAN,
    sequence_id           VARCHAR,
    creator_id            BIGINT
"""

_COLUMNS = [
    "image_id",
    "image_blob",
    "image_sha256",
    "image_mime",
    "image_bytes",
    "image_width",
    "image_height",
    "downloaded_at",
    "source",
    "mapillary_bbox_query",
    "lon",
    "lat",
    "compass_angle",
    "captured_at",
    "camera_type",
    "is_pano",
    "sequence_id",
    "creator_id",
]


@dataclass
class CacheRow:
    image_id: str
    image_blob: bytes | None
    image_sha256: str
    image_mime: str
    image_bytes: int
    image_width: int | None
    image_height: int | None
    downloaded_at: datetime
    source: str
    mapillary_bbox_query: str
    lon: float | None
    lat: float | None
    compass_angle: float | None
    captured_at: datetime | None
    camera_type: str | None
    is_pano: bool | None
    sequence_id: str | None
    creator_id: int | None


def _create_staging(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE cache_images_staging ({_SCHEMA_SQL})")


def _row_tuple(row: CacheRow) -> tuple:
    return tuple(getattr(row, f.name) for f in dc_fields(row))


def ensure_sentinel(cache_dir: Path = CACHE_DIR) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    sentinel = cache_dir / SENTINEL_NAME
    if sentinel.exists():
        return
    con = duckdb.connect(":memory:")
    try:
        _create_staging(con)
        placeholder = CacheRow(
            image_id="__sentinel__",
            image_blob=None,
            image_sha256="",
            image_mime="image/jpeg",
            image_bytes=0,
            image_width=None,
            image_height=None,
            downloaded_at=datetime.now(UTC).replace(tzinfo=None),
            source="mapillary",
            mapillary_bbox_query="",
            lon=None,
            lat=None,
            compass_angle=None,
            captured_at=None,
            camera_type=None,
            is_pano=None,
            sequence_id=None,
            creator_id=None,
        )
        placeholders = ", ".join(["?"] * len(_COLUMNS))
        con.execute(
            f"INSERT INTO cache_images_staging VALUES ({placeholders})",
            _row_tuple(placeholder),
        )
        tmp = sentinel.with_suffix(sentinel.suffix + ".tmp")
        con.execute(
            f"COPY (SELECT * FROM cache_images_staging) TO '{tmp.as_posix()}' "
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


def build_cache_row(meta: ImageMeta, blob: bytes, *, bbox_query: str) -> CacheRow:
    sha = hashlib.sha256(blob).hexdigest()
    # Mapillary epoch ms field, parsed to naive UTC for DuckDB TIMESTAMP.
    captured_ms = getattr(meta, "captured_at_ms", None)
    captured_at: datetime | None = None
    if captured_ms is not None:
        captured_at = datetime.fromtimestamp(captured_ms / 1000, tz=UTC).replace(tzinfo=None)
    downloaded_at = datetime.now(UTC).replace(tzinfo=None)
    return CacheRow(
        image_id=str(meta.image_id),
        image_blob=blob,
        image_sha256=sha,
        image_mime="image/jpeg",
        image_bytes=len(blob),
        image_width=getattr(meta, "width", None),
        image_height=getattr(meta, "height", None),
        downloaded_at=downloaded_at,
        source="mapillary",
        mapillary_bbox_query=bbox_query,
        lon=getattr(meta, "lon", None),
        lat=getattr(meta, "lat", None),
        compass_angle=getattr(meta, "compass_angle", None),
        captured_at=captured_at,
        camera_type=getattr(meta, "camera_type", None),
        is_pano=getattr(meta, "is_pano", None),
        sequence_id=getattr(meta, "sequence_id", None),
        creator_id=getattr(meta, "creator_id", None),
    )


def write_shard(rows: Sequence[CacheRow], cache_dir: Path = CACHE_DIR) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    # ISO8601 UTC with no colons, Z suffix, e.g. 2026-04-30T13-22-05Z.
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    final = cache_dir / f"cache_part_{ts}.parquet"
    tmp = final.with_suffix(final.suffix + ".tmp")
    con = duckdb.connect(":memory:")
    try:
        _create_staging(con)
        if rows:
            placeholders = ", ".join(["?"] * len(_COLUMNS))
            con.executemany(
                f"INSERT INTO cache_images_staging VALUES ({placeholders})",
                [_row_tuple(r) for r in rows],
            )
        con.execute(
            f"COPY (SELECT * FROM cache_images_staging) TO '{tmp.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9, ROW_GROUP_SIZE 1024)"
        )
        os.replace(tmp, final)
    finally:
        con.close()
    return final


def stats(cache_dir: Path = CACHE_DIR) -> dict:
    ensure_sentinel(cache_dir)
    glob = (cache_dir / "cache_part_*.parquet").as_posix()
    con = duckdb.connect(":memory:")
    try:
        n_shards = len(list(cache_dir.glob("cache_part_*.parquet")))
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS n_rows,
                COALESCE(SUM(image_bytes), 0) AS total_bytes,
                MIN(captured_at) AS oldest_captured_at,
                MAX(captured_at) AS newest_captured_at
            FROM read_parquet('{glob}', union_by_name=true)
            WHERE image_id <> '__sentinel__'
            """
        ).fetchone()
    finally:
        con.close()
    n_rows, total_bytes, oldest, newest = row if row else (0, 0, None, None)
    return {
        "n_rows": int(n_rows or 0),
        "n_shards": n_shards,
        "total_bytes": int(total_bytes or 0),
        "oldest_captured_at": oldest,
        "newest_captured_at": newest,
    }
