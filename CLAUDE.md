# CLAUDE.md

Quick orientation for future chats. Read `PLAN.md` for the full spec.

## What we're cooking

A small POC that pulls Mapillary street-level images for a London AOI (Camden
Town primary, Borough Market fallback), runs each frame through Meta FAIR's
**TRIBE v2** brain-encoding model in vision-only mode, and writes one
**GeoParquet 2.0** file with cortical activity per HCP MMP1 parcel attached to
each image's GPS point.

## How

```
Mapillary bbox  →  anti-join cache  →  download new only
                                       ↓
                             cache_images/cache_part_*.parquet
                             (JPEG inlined as BLOB + metadata)
                                       ↓
                  jpeg_to_static_clip (1 still → 30 s MP4)
                                       ↓
                       TribeRunner (V-JEPA2 ViT-G video tower)
                                       ↓
                  inference_results (brain_activity, top_regions)
                                       ↓
                  cache_inference/cache_part_*.parquet (resumable)
                                       ↓
            DuckDB COPY (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
                                       ↓
                          london_aoi.parquet
                  (GEOMETRY('OGC:CRS84'), native Parquet type)
```

Cache-first: Mapillary is hit at most once per `image_id`. Inference cache
keeps re-runs cheap when the model output for an image already exists.

## Why

- **Cache as Parquet shards with inline BLOBs.** Survives Mac ↔ Colab via a
  private HF dataset repo. Anti-join in DuckDB is the only dedup.
- **GeoParquet 2.0 native** (`GEOPARQUET_VERSION 'V2'`, native `GEOMETRY`
  logical type, explicit OGC:CRS84). Written by DuckDB 1.5.x core, no GDAL.
- **Vision-only TRIBE.** Sidesteps the Llama 3.2 license gate. Static clip is
  a known caveat for MT/V5 motion areas, ventral-stream regions are fine.
- **uv** for env management. `requires-python >=3.11`. Heavy ML deps
  (`tribev2`, `mne`, `nilearn`, `torch`, `torchvision`) live in the `tribe`
  optional extra so the lightweight cache-only path stays cheap to install.

## Where to look

| If you need... | Look in |
|---|---|
| Full spec, AOI math, schema decisions, milestones | `PLAN.md` |
| End-to-end orchestration, the `hnc-run` CLI | `src/pipeline.py` |
| Mapillary Graph API client, retry, CDN download | `src/mapillary_client.py` |
| Image cache shard reader/writer + anti-join | `src/cache.py` |
| Inference checkpoint cache (resume mid-batch) | `src/inference_cache.py` |
| TRIBE v2 model load + predict wrapper | `src/tribe_runner.py` |
| HCP MMP1 parcel summarization, alias top-K | `src/roi_summary.py` |
| Static-clip MP4 generation from JPEG | `src/frame_to_clip.py` |
| **GeoParquet 2.0 deliverable writer + verifier** | `src/geoparquet_writer.py` |
| `.env` loader (`MAPILLARY_ACCESS_TOKEN`, `HF_TOKEN`) | `src/env.py` |
| Smoke tests (no network, no GPU) | `tests/test_smoke.py` |
| Colab runner | `notebooks/colab.ipynb` |
| AOI bbox + true-square formula at London latitude | `src/aoi.py` |

## Conventions

- Single source of truth for env: `.env` at repo root, never a legacy parser.
- Schema column order in `cache.py` mirrors `PLAN.md` §4 verbatim.
- Always pass `GEOPARQUET_VERSION 'V2'` explicitly (DuckDB defaults to V1).
- Always use `ST_SetCRS(ST_Point(lon, lat), 'OGC:CRS84')` for the deliverable
  geometry column. `SET geometry_always_xy = true` on any spatial session.
- Lon/lat order. Always. `ST_Point(lon, lat)`, never `(lat, lon)`.
- `MapillaryClient.list_images_in_bbox` follows `paging.next` until either the
  caller's total `limit` is reached or the server runs out, so `--max-images`
  is a real total cap, not a per-page cap.
- Mapillary's bbox spatial index leaks rows up to ~75 m outside the requested
  bbox. The pipeline re-checks `lon`/`lat` immediately after the API call and
  logs each `out_of_bbox` drop, so out-of-AOI frames never enter the cache.
- Any silent filter is a bug. `_materialise_working_set` logs every dropped
  image_id with a reason (`not_in_cache` / `null_blob`), the download loop
  logs every failed `image_id`, the inference loop logs every TRIBE failure.
- Inference is **batched and adaptive**. `TribeRunner` auto-picks
  `batch_size` from VRAM (T4 16 GB → 1, L4 24 GB → 4, A100 40 GB → 12,
  A100 80 GB / H100 80 GB → 24) and wraps the forward pass in
  `torch.autocast(bfloat16)` on CUDA. The pipeline groups clips into chunks
  of `runner.batch_size`, encodes each chunk's MP4s into one tempdir, and
  calls `runner.predict_clips()` once per chunk so V-JEPA2 ViT-G actually
  fills the GPU. A failed batch falls back to per-clip inference so one bad
  image doesn't kill the whole chunk.
- Run `uv sync --all-extras` to set up; `uv run pytest` and `uv run ruff check .`
  to verify before committing.

## Pinning notes

- `numpy>=2.2,<2.5` because tribev2 hard-pins `numpy==2.2.6`.
- `duckdb>=1.5.2,<2` because the v2.0 spatial axis-order default flips.
- `tribev2` pinned to commit `72399081ed3f1040c4d996cefb2864a4c46f5b8e` since
  upstream has no tags and may force-push `main`.
- `torch>=2.5.1,<2.7`, `torchvision>=0.20,<0.22` to match upstream caps.
- On Blackwell GPUs (sm_120) the default torch wheels miss kernels, see the
  cu128 reinstall block in `notebooks/colab.ipynb` cell 6.

## License

Source: CC BY 4.0. Runtime artifacts inherit upstream terms (TRIBE v2
CC-BY-NC-4.0, Llama 3.2 community, Mapillary terms). See `NOTICE.md`.
