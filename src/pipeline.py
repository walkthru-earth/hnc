"""End-to-end orchestrator for the Mapillary -> TRIBE v2 -> GeoParquet 2.0 POC."""

from __future__ import annotations

import logging
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import typer
from tqdm import tqdm

from .aoi import BOROUGH_MARKET, CAMDEN_TOWN, BBox, true_square_bbox

logger = logging.getLogger("pipeline")

app = typer.Typer(add_completion=False, no_args_is_help=False)


def _setup_logging() -> None:
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)sZ %(levelname)s %(name)s %(message)s"))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)


def _resolve_bbox(bbox_name: str, side_m: float, lon0: float | None, lat0: float | None) -> BBox:
    name = (bbox_name or "").strip().lower()
    if name == "camden":
        return CAMDEN_TOWN
    if name == "borough":
        return BOROUGH_MARKET
    if lon0 is None or lat0 is None:
        raise ValueError(
            f"unknown bbox_name {bbox_name!r}, pass lon0/lat0 to derive a true-square bbox"
        )
    return true_square_bbox(lon0, lat0, side_m)


def _parse_start_captured(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if "T" in s:
            return (
                datetime.fromisoformat(s.replace("Z", "+00:00"))
                .astimezone(UTC)
                .replace(tzinfo=None)
            )
        return (
            datetime.strptime(s, "%Y-%m-%d")
            .replace(tzinfo=UTC)
            .astimezone(UTC)
            .replace(tzinfo=None)
        )
    except ValueError:
        logger.warning("could not parse start_captured_at %r, skipping filter", s)
        return None


def _collapse_segments(preds: np.ndarray) -> np.ndarray:
    arr = np.asarray(preds, dtype=np.float32)
    if arr.ndim == 1:
        return arr
    return arr.mean(axis=0).astype(np.float32)


def _safe_collapse(preds: np.ndarray) -> np.ndarray:
    from . import roi_summary  # noqa: F401, lazy import

    fn = getattr(roi_summary, "collapse_segments", None)
    if callable(fn):
        return np.asarray(fn(preds), dtype=np.float32)
    return _collapse_segments(preds)


def _safe_parcel_means_dict(vertex_vec: np.ndarray) -> dict[str, float]:
    from . import roi_summary

    fn = getattr(roi_summary, "parcel_means", None)
    if not callable(fn):
        raise RuntimeError("roi_summary.parcel_means not available")
    raw = fn(vertex_vec)
    if not isinstance(raw, dict):
        raise RuntimeError(
            f"roi_summary.parcel_means must return dict[str, float], got {type(raw).__name__}"
        )
    return {str(k): float(v) for k, v in raw.items()}


def _safe_top_k(means: dict[str, float], k: int = 10) -> list[dict[str, Any]]:
    from . import roi_summary

    fn = getattr(roi_summary, "top_k_aliases", None)
    if not callable(fn):
        raise RuntimeError("roi_summary.top_k_aliases not available")
    raw = fn(means, k=k)
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and "name" in item and "score" in item:
            out.append({"name": str(item["name"]), "score": float(item["score"])})
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            out.append({"name": str(item[0]), "score": float(item[1])})
    return out


def _create_inference_table(
    con: duckdb.DuckDBPyConnection, table: str = "inference_results"
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table} (
            image_id        VARCHAR,
            brain_activity  FLOAT[],
            top_regions     STRUCT(name VARCHAR, score FLOAT)[]
        )
        """
    )


def _insert_inference_row(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    image_id: str,
    brain_activity: np.ndarray,
    top_regions: list[dict[str, Any]],
) -> None:
    activity_list = [float(x) for x in np.asarray(brain_activity, dtype=np.float32).tolist()]
    struct_list = [{"name": r["name"], "score": float(r["score"])} for r in top_regions]
    con.execute(
        f"INSERT INTO {table} (image_id, brain_activity, top_regions) VALUES (?, ?, ?)",
        [image_id, activity_list, struct_list],
    )


def _materialise_working_set(
    con: duckdb.DuckDBPyConnection,
    *,
    cache_glob: str,
    candidate_ids: list[str],
) -> list[tuple[str, bytes]]:
    """Pull the freshly-cached blobs for `candidate_ids` and log any drop.

    Bbox pre-filtering happens upstream in the pipeline, here we only refuse
    rows that are missing from cache or have a NULL blob. Both situations are
    bugs we want loud, so log each dropped image_id.
    """
    if not candidate_ids:
        return []
    placeholders = ", ".join(["?"] * len(candidate_ids))
    sql = f"""
        SELECT image_id, image_blob
        FROM read_parquet('{cache_glob}', union_by_name=true)
        WHERE image_id <> '__sentinel__'
          AND image_id IN ({placeholders})
    """
    rows = con.execute(sql, candidate_ids).fetchall()
    by_id: dict[str, bytes | None] = {}
    for r in rows:
        iid = str(r[0])
        blob = bytes(r[1]) if r[1] is not None else None
        # Last write wins, dedup is fine since image_blob is content-addressed.
        by_id[iid] = blob

    out: list[tuple[str, bytes]] = []
    for iid in candidate_ids:
        if iid not in by_id:
            logger.warning("materialise drop image_id=%s reason=not_in_cache", iid)
            continue
        blob = by_id[iid]
        if blob is None:
            logger.warning("materialise drop image_id=%s reason=null_blob", iid)
            continue
        out.append((iid, blob))
    return out


def run(
    bbox_name: str = "camden",
    side_m: float = 300.0,
    lon0: float | None = None,
    lat0: float | None = None,
    max_images: int = 1000,
    start_captured_at: str | None = "2023-04-30",
    out: Path = Path("london_aoi.parquet"),
    tribe_repo: str = "facebook/tribev2",
    device: str = "auto",
) -> dict[str, Any]:
    _setup_logging()
    timings: dict[str, float] = {}
    counts: dict[str, int] = {
        "candidates": 0,
        "cached": 0,
        "downloaded": 0,
        "inference_cache_hit": 0,
        "inferred": 0,
        "written": 0,
    }

    # Lazy imports keep import-order coupling loose with sibling modules.
    from .cache import build_cache_row, ensure_sentinel, filter_new, write_shard
    from .frame_to_clip import jpeg_to_static_clip
    from .geoparquet_writer import verify_geoparquet_v2, write_geoparquet_v2
    from .inference_cache import (
        InferenceRow,
    )
    from .inference_cache import (
        cached_ids as inference_cached_ids,
    )
    from .inference_cache import (
        ensure_sentinel as ensure_inference_sentinel,
    )
    from .inference_cache import (
        load_into_table as load_inference_into_table,
    )
    from .inference_cache import (
        write_shard as write_inference_shard,
    )
    from .mapillary_client import MapillaryClient
    from .tribe_runner import TribeRunner

    # 1. Resolve AOI bbox.
    t0 = time.monotonic()
    bbox = _resolve_bbox(bbox_name, side_m, lon0, lat0)
    bbox.assert_under_mapillary_limit()
    logger.info("aoi bbox=%s area=%.6f sq_deg", bbox.as_mapillary_str(), bbox.area_sq_deg())
    timings["resolve_bbox_s"] = time.monotonic() - t0

    # 2. Ensure cache sentinel exists.
    t0 = time.monotonic()
    ensure_sentinel()
    timings["ensure_sentinel_s"] = time.monotonic() - t0

    # 3. Query Mapillary for candidate ids.
    t0 = time.monotonic()
    start_dt = _parse_start_captured(start_captured_at)
    with MapillaryClient() as mly:
        api_limit = max_images if max_images > 0 else 2000
        raw_metas = mly.list_images_in_bbox(
            bbox,
            start_captured_at=start_dt,
            is_pano=False,
            limit=api_limit,
        )
        # Mapillary's spatial index leaks rows whose computed_geometry sits
        # just outside the requested bbox, drop them now so we don't waste
        # bytes downloading images that won't be in the deliverable.
        metas = []
        for m in raw_metas:
            if (
                bbox.west <= m.lon <= bbox.east
                and bbox.south <= m.lat <= bbox.north
            ):
                metas.append(m)
            else:
                logger.warning(
                    "mapillary drop image_id=%s reason=out_of_bbox lon=%.6f lat=%.6f",
                    m.image_id,
                    m.lon,
                    m.lat,
                )
        if max_images > 0:
            metas = metas[:max_images]
        counts["candidates"] = len(metas)
        candidate_ids = [m.image_id for m in metas]
        logger.info(
            "mapillary candidates=%d (after is_pano=False + bbox-recheck, "
            "raw=%d, max_images=%d)",
            len(metas),
            len(raw_metas),
            max_images,
        )
        timings["mapillary_query_s"] = time.monotonic() - t0

        # 4. Anti-join against cache.
        t0 = time.monotonic()
        new_ids = set(filter_new(candidate_ids))
        n_new = len(new_ids)
        n_cached = len(candidate_ids) - n_new
        counts["cached"] = n_cached
        logger.info(
            "anti-join cache_hit=%d cache_miss=%d (api-hit-savings=%d)", n_cached, n_new, n_cached
        )
        timings["anti_join_s"] = time.monotonic() - t0

        # 5. Download new images.
        t0 = time.monotonic()
        rows_to_write = []
        bbox_query = bbox.as_mapillary_str()
        for meta in tqdm([m for m in metas if m.image_id in new_ids], desc="download", unit="img"):
            try:
                blob = mly.download_image(meta.thumb_2048_url)
                rows_to_write.append(build_cache_row(meta, blob, bbox_query=bbox_query))
            except Exception as exc:
                logger.warning("download failed image_id=%s err=%s", meta.image_id, exc)
                continue
        counts["downloaded"] = len(rows_to_write)
        if rows_to_write:
            shard = write_shard(rows_to_write)
            logger.info("wrote shard=%s rows=%d", shard.as_posix(), len(rows_to_write))
        else:
            logger.info("no new images to write")
        timings["download_s"] = time.monotonic() - t0

    # 6. Materialise the working set in DuckDB.
    t0 = time.monotonic()
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial; LOAD spatial;")
    cache_glob = "cache_images/cache_part_*.parquet"
    working = _materialise_working_set(
        con, cache_glob=cache_glob, candidate_ids=candidate_ids
    )
    logger.info("working set rows=%d", len(working))
    timings["materialise_s"] = time.monotonic() - t0

    # 7. Inference, with on-disk checkpoint cache so a re-run skips images
    # whose brain_activity is already persisted.
    t0 = time.monotonic()
    _create_inference_table(con)
    ensure_inference_sentinel()
    working_ids = [iid for iid, _ in working]
    already_inferred = inference_cached_ids(con) & set(working_ids)
    to_infer = [(iid, blob) for iid, blob in working if iid not in already_inferred]
    counts["inference_cache_hit"] = len(already_inferred)
    logger.info("inference cache_hit=%d cache_miss=%d", len(already_inferred), len(to_infer))
    if already_inferred:
        load_inference_into_table(
            con, table="inference_results", image_ids=sorted(already_inferred)
        )

    n_inferred = 0
    flush_every = max(5, 0)
    pending: list[InferenceRow] = []

    if to_infer:
        runner = TribeRunner(repo_id=tribe_repo, device=device)
        runner.load()
        chunk_size = max(1, runner.batch_size)
        logger.info(
            "inference batched device=%s chunk_size=%d total=%d",
            runner.device,
            chunk_size,
            len(to_infer),
        )
        for chunk_start in tqdm(
            range(0, len(to_infer), chunk_size), desc="inference", unit="chunk"
        ):
            chunk = to_infer[chunk_start : chunk_start + chunk_size]
            with tempfile.TemporaryDirectory(prefix="tribe_clip_") as td:
                td_path = Path(td)
                items: list[tuple[str, Path]] = []
                encoded_blobs: dict[str, bytes] = {}
                for image_id, blob in chunk:
                    try:
                        clip_path = td_path / f"{image_id}.mp4"
                        jpeg_to_static_clip(blob, clip_path)
                        items.append((image_id, clip_path))
                        encoded_blobs[image_id] = blob
                    except Exception as exc:
                        logger.warning(
                            "encode failed image_id=%s err=%s", image_id, exc
                        )
                        continue
                if not items:
                    continue
                try:
                    preds_by_id = runner.predict_clips(items)
                except Exception as exc:
                    logger.warning(
                        "inference batch failed n=%d err=%s, falling back to per-clip",
                        len(items),
                        exc,
                    )
                    preds_by_id = {}
                    for image_id, clip_path in items:
                        try:
                            single = runner.predict_clips([(image_id, clip_path)])
                            preds_by_id.update(single)
                        except Exception as exc_inner:
                            logger.warning(
                                "inference failed image_id=%s err=%s",
                                image_id,
                                exc_inner,
                            )
                for image_id, _clip_path in items:
                    preds = preds_by_id.get(image_id)
                    if preds is None or preds.size == 0:
                        logger.warning(
                            "inference no segments image_id=%s", image_id
                        )
                        continue
                    try:
                        collapsed = _safe_collapse(preds)
                        means_dict = _safe_parcel_means_dict(collapsed)
                        top_regions = _safe_top_k(means_dict, k=10)
                        activity_list = [
                            float(x)
                            for x in np.asarray(collapsed, dtype=np.float32).tolist()
                        ]
                        _insert_inference_row(
                            con,
                            table="inference_results",
                            image_id=image_id,
                            brain_activity=np.asarray(collapsed, dtype=np.float32),
                            top_regions=top_regions,
                        )
                        pending.append(
                            InferenceRow(
                                image_id=image_id,
                                brain_activity=activity_list,
                                top_regions=top_regions,
                                inferred_at=datetime.now(UTC).replace(tzinfo=None),
                                model_repo=tribe_repo,
                            )
                        )
                        n_inferred += 1
                    except Exception as exc:
                        logger.warning(
                            "post-process failed image_id=%s err=%s", image_id, exc
                        )
                        continue
            if len(pending) >= flush_every:
                write_inference_shard(pending)
                pending = []
        if pending:
            write_inference_shard(pending)
            pending = []

    counts["inferred"] = n_inferred
    timings["inference_s"] = time.monotonic() - t0

    if (n_inferred + len(already_inferred)) == 0:
        raise RuntimeError(
            f"All {len(working)} inference attempts failed and no cached inference "
            "results matched. The deliverable would be empty and the GeoParquet "
            "write would not emit valid geometry metadata. Check the per-image "
            "warnings above. On Blackwell GPUs (sm_120), the default torch wheels "
            "lack matching kernels, install the cu128 build into .venv with "
            "`uv pip install -p .venv/bin/python --reinstall "
            "--index-url https://download.pytorch.org/whl/cu128 torch torchvision`."
        )

    # 8. (inference_results is already populated above)

    # 9. Write deliverable.
    t0 = time.monotonic()
    out_path = Path(out)
    write_geoparquet_v2(
        con,
        cache_glob=cache_glob,
        inference_table="inference_results",
        out_path=out_path,
        bbox=bbox,
    )
    timings["write_s"] = time.monotonic() - t0

    # 10. Verify.
    t0 = time.monotonic()
    verify = verify_geoparquet_v2(out_path)
    counts["written"] = int(verify.get("n_rows", 0))
    logger.info("verify=%s", verify)
    timings["verify_s"] = time.monotonic() - t0
    con.close()

    summary = {"counts": counts, "timings_s": timings, "out": out_path.as_posix(), "verify": verify}
    print(
        f"summary: candidates={counts['candidates']} cached={counts['cached']} "
        f"downloaded={counts['downloaded']} "
        f"inference_cache_hit={counts['inference_cache_hit']} "
        f"inferred={counts['inferred']} written={counts['written']} "
        f"timings={ {k: round(v, 2) for k, v in timings.items()} } out={out_path.as_posix()}"
    )
    return summary


@app.command()
def main(
    bbox_name: str = typer.Option("camden", help="'camden', 'borough', or 'custom' with lon0/lat0"),
    side_m: float = typer.Option(
        300.0, help="True-square side length in meters when using lon0/lat0"
    ),
    lon0: float | None = typer.Option(None, help="Center longitude for custom bbox"),
    lat0: float | None = typer.Option(None, help="Center latitude for custom bbox"),
    max_images: int = typer.Option(1000, help="Maximum images to process"),
    start_captured_at: str | None = typer.Option(
        "2023-04-30", help="Earliest captured_at, ISO date or datetime"
    ),
    out: Path = typer.Option(Path("london_aoi.parquet"), help="Output GeoParquet 2.0 path"),
    tribe_repo: str = typer.Option("facebook/tribev2", help="HF repo id for TRIBE v2 weights"),
    device: str = typer.Option("auto", help="Torch device, 'auto' | 'cpu' | 'mps' | 'cuda'"),
) -> None:
    run(
        bbox_name=bbox_name,
        side_m=side_m,
        lon0=lon0,
        lat0=lat0,
        max_images=max_images,
        start_captured_at=start_captured_at,
        out=out,
        tribe_repo=tribe_repo,
        device=device,
    )


if __name__ == "__main__":
    app()
