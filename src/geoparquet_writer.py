"""GeoParquet 2.0 deliverable writer, native DuckDB COPY, no GDAL."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from .aoi import BBox

DELIVERABLE_PATH: Path = Path("london_aoi.parquet")


def write_geoparquet_v2(
    con: duckdb.DuckDBPyConnection,
    *,
    cache_glob: str = "cache_images/cache_part_*.parquet",
    inference_table: str = "inference_results",
    out_path: Path = DELIVERABLE_PATH,
    bbox: BBox | None = None,
) -> Path:
    con.execute("INSTALL spatial; LOAD spatial;")

    where_clauses = ["c.image_id <> '__sentinel__'"]
    if bbox is not None:
        where_clauses.append(
            f"c.lon BETWEEN {bbox.west} AND {bbox.east} "
            f"AND c.lat BETWEEN {bbox.south} AND {bbox.north}"
        )
    where_sql = " AND ".join(where_clauses)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    select_sql = f"""
        SELECT
            c.image_id,
            c.image_blob,
            c.image_mime,
            c.captured_at,
            c.compass_angle,
            c.camera_type,
            ST_Point(c.lon, c.lat) AS geom,
            inf.brain_activity,
            inf.top_regions
        FROM read_parquet('{cache_glob}', union_by_name=true) c
        JOIN {inference_table} inf USING (image_id)
        WHERE {where_sql}
    """

    copy_sql = f"""
        COPY ({select_sql}) TO '{out_path.as_posix()}' (
            FORMAT PARQUET,
            GEOPARQUET_VERSION 'V2',
            COMPRESSION 'ZSTD',
            COMPRESSION_LEVEL 9,
            ROW_GROUP_SIZE 2048
        )
    """
    con.execute(copy_sql)
    return out_path


def verify_geoparquet_v2(path: Path) -> dict:
    path = Path(path)
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL spatial; LOAD spatial;")

        row = con.execute(
            f"SELECT decode(value) FROM parquet_kv_metadata('{path.as_posix()}') WHERE key = 'geo'"
        ).fetchone()
        if not row or row[0] is None:
            raise AssertionError("no 'geo' key found in parquet_kv_metadata")
        geo_blob = row[0]
        geo_str = (
            geo_blob.decode("utf-8") if isinstance(geo_blob, (bytes, bytearray)) else str(geo_blob)
        )
        geo = json.loads(geo_str)
        version = str(geo.get("version", ""))
        primary_column = str(geo.get("primary_column", ""))

        # Canonical GeoParquet V2 source for geometry types is the geo JSON
        # itself, geo.columns[<primary>].geometry_types. DuckDB also exposes a
        # geo_types column on parquet_metadata(), but it can be empty even
        # when the file is a valid GeoParquet V2, so we trust the JSON.
        columns_meta = geo.get("columns", {}) or {}
        primary_meta = columns_meta.get(primary_column, {}) or {}
        raw_types = primary_meta.get("geometry_types", []) or []
        geo_types_list: list[str] = [str(t).lower() for t in raw_types]

        shape_row = con.execute(
            f"SELECT COUNT(*) AS n_rows, "
            f"(SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}'))) AS n_columns "
            f"FROM read_parquet('{path.as_posix()}')"
        ).fetchone()
        n_rows = int(shape_row[0]) if shape_row else 0
        n_columns = int(shape_row[1]) if shape_row else 0

        # GeoParquet V2 allows an empty geometry_types array, meaning the
        # writer chose not to constrain the type. In that case we fall back
        # to inspecting the data itself.
        if not geo_types_list and n_rows > 0 and primary_column:
            sampled = con.execute(
                f"SELECT DISTINCT lower(ST_GeometryType({primary_column})) "
                f"FROM read_parquet('{path.as_posix()}') WHERE {primary_column} IS NOT NULL"
            ).fetchall()
            geo_types_list = [str(r[0]).removeprefix("st_") for r in sampled if r and r[0]]
    finally:
        con.close()

    if version != "2.0.0":
        raise AssertionError(f"expected geo.version == '2.0.0', got {version!r}")
    if geo_types_list and geo_types_list != ["point"]:
        raise AssertionError(f"expected geo_types == ['point'], got {geo_types_list!r}")

    return {
        "version": version,
        "primary_column": primary_column,
        "geo_types": geo_types_list,
        "n_rows": n_rows,
        "n_columns": n_columns,
    }
