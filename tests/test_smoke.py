from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.aoi import (
    BOROUGH_MARKET,
    CAMDEN_TOWN,
    MAPILLARY_BBOX_MAX_AREA_SQ_DEG,
    BBox,
    true_square_bbox,
)
from src.cache import (
    SENTINEL_NAME,
    CacheRow,
    build_cache_row,
    cached_ids,
    ensure_sentinel,
    filter_new,
    stats,
    write_shard,
)

# ---- aoi.BBox -----------------------------------------------------------


def test_bbox_str_format():
    b = CAMDEN_TOWN
    s = b.as_mapillary_str()
    parts = s.split(",")
    assert len(parts) == 4
    assert parts == ["-0.146500", "51.540500", "-0.142200", "51.543000"]


def test_bbox_area_under_limit():
    assert CAMDEN_TOWN.area_sq_deg() < MAPILLARY_BBOX_MAX_AREA_SQ_DEG
    assert BOROUGH_MARKET.area_sq_deg() < MAPILLARY_BBOX_MAX_AREA_SQ_DEG
    CAMDEN_TOWN.assert_under_mapillary_limit()
    BOROUGH_MARKET.assert_under_mapillary_limit()


def test_bbox_oversize_raises():
    huge = BBox(-1.0, 51.0, 1.0, 52.0)
    with pytest.raises(ValueError):
        huge.assert_under_mapillary_limit()


def test_true_square_bbox_at_london_lat():
    # Camden centroid, 300 m square. PLAN section 2: dlat ~0.002695, dlon ~0.004332.
    lon0, lat0 = -0.1444, 51.5415
    b = true_square_bbox(lon0, lat0, 300.0)
    dlat = b.north - b.south
    dlon = b.east - b.west
    assert math.isclose(dlat, 300 / 111320, rel_tol=1e-3)
    expected_dlon = 300 / (111320 * math.cos(math.radians(lat0)))
    assert math.isclose(dlon, expected_dlon, rel_tol=1e-3)
    # Skinny lon-degree at lat 51.5: dlon should be ~1.6x dlat.
    assert dlon / dlat == pytest.approx(1 / math.cos(math.radians(lat0)), rel=1e-3)


# ---- cache.ensure_sentinel + cached_ids + stats -------------------------


def test_sentinel_creates_glob_match(tmp_path: Path):
    cache_dir = tmp_path / "cache_images"
    ensure_sentinel(cache_dir)
    sentinel = cache_dir / SENTINEL_NAME
    assert sentinel.exists()
    # idempotent
    ensure_sentinel(cache_dir)
    assert sentinel.exists()


def test_cached_ids_excludes_sentinel(tmp_path: Path):
    import duckdb

    cache_dir = tmp_path / "cache_images"
    ensure_sentinel(cache_dir)
    con = duckdb.connect(":memory:")
    try:
        ids = cached_ids(con, cache_dir)
    finally:
        con.close()
    assert ids == set()


def test_filter_new_with_empty_cache(tmp_path: Path):
    cache_dir = tmp_path / "cache_images"
    ensure_sentinel(cache_dir)
    candidates = ["a", "b", "c"]
    new = filter_new(candidates, cache_dir)
    assert new == ["a", "b", "c"]


def test_round_trip_one_shard(tmp_path: Path):
    """Write one row, read it back, confirm the anti-join sees it."""
    import duckdb

    cache_dir = tmp_path / "cache_images"
    ensure_sentinel(cache_dir)

    row = CacheRow(
        image_id="42",
        image_blob=b"\xff\xd8\xff\xe0fake",
        image_sha256="abc",
        image_mime="image/jpeg",
        image_bytes=8,
        image_width=2048,
        image_height=1536,
        downloaded_at=datetime.now(UTC).replace(tzinfo=None),
        source="mapillary",
        mapillary_bbox_query=CAMDEN_TOWN.as_mapillary_str(),
        lon=-0.144,
        lat=51.541,
        compass_angle=180.5,
        captured_at=datetime(2024, 5, 1, 12, 0, 0),
        camera_type="perspective",
        is_pano=False,
        sequence_id="seq-xyz",
        creator_id=999,
    )
    write_shard([row], cache_dir)

    con = duckdb.connect(":memory:")
    try:
        ids = cached_ids(con, cache_dir)
    finally:
        con.close()

    assert ids == {"42"}
    assert filter_new(["42", "100"], cache_dir) == ["100"]

    s = stats(cache_dir)
    assert s["n_rows"] == 1
    assert s["total_bytes"] == 8
    assert s["n_shards"] >= 2  # sentinel + the new shard


# ---- build_cache_row from a fake ImageMeta ------------------------------


def test_build_cache_row_parses_epoch_ms():
    from src.mapillary_client import ImageMeta

    meta = ImageMeta(
        image_id="123",
        lon=-0.144,
        lat=51.541,
        compass_angle=90.0,
        captured_at_ms=1714564800000,  # 2024-05-01T12:00:00Z
        camera_type="perspective",
        is_pano=False,
        sequence_id="seq",
        creator_id=7,
        width=2048,
        height=1536,
        thumb_2048_url="https://example/x.jpg",
    )
    blob = b"\xff\xd8\xff\xe0test"
    row = build_cache_row(meta, blob, bbox_query=CAMDEN_TOWN.as_mapillary_str())

    assert row.image_id == "123"
    assert row.image_bytes == len(blob)
    assert row.image_sha256 != ""
    assert row.captured_at == datetime(2024, 5, 1, 12, 0, 0)
    assert row.lon == -0.144 and row.lat == 51.541
    assert row.is_pano is False
    assert row.mapillary_bbox_query == CAMDEN_TOWN.as_mapillary_str()


# ---- MapillaryClient instantiation (no network) ------------------------


def test_mapillary_client_picks_up_token_from_env():
    """Confirms the dotenv loader resolves MAPILLARY_ACCESS_TOKEN. No network call."""
    import os

    from src.mapillary_client import MapillaryClient

    # Smoke: passing an explicit token always works.
    c = MapillaryClient(access_token="MLY|fake|token")
    assert c._token == "MLY|fake|token"
    c.close()

    # If the env loader resolves a token from .env, instantiation works.
    if os.environ.get("MAPILLARY_ACCESS_TOKEN") or Path(".env").exists():
        c = MapillaryClient()
        assert c._token  # non-empty
        c.close()


def test_mapillary_client_raises_without_token(monkeypatch):
    """When the token resolver returns nothing, the client must refuse to construct."""
    from src import mapillary_client

    monkeypatch.setattr(mapillary_client, "_resolve_token", lambda: None)
    with pytest.raises(RuntimeError, match="MAPILLARY_ACCESS_TOKEN"):
        mapillary_client.MapillaryClient()
