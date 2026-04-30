from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

from .aoi import BBox

GRAPH_URL = "https://graph.mapillary.com/images"
DEFAULT_FIELDS = (
    "id,captured_at,computed_geometry,computed_compass_angle,"
    "camera_type,is_pano,sequence,creator,width,height,thumb_2048_url"
)
MAX_RETRIES = 5
BACKOFF_BASE_S = 1.0


@dataclass
class ImageMeta:
    image_id: str
    lon: float
    lat: float
    compass_angle: float | None
    captured_at_ms: int
    camera_type: str | None
    is_pano: bool
    sequence_id: str | None
    creator_id: int | None
    width: int | None
    height: int | None
    thumb_2048_url: str


def _resolve_token() -> str | None:
    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=repo_root / ".env")
    tok = os.environ.get("MAPILLARY_ACCESS_TOKEN")
    return tok.strip() if tok else None


def _to_iso_z(dt: datetime) -> str:
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class MapillaryClient:
    def __init__(self, access_token: str | None = None, timeout: float = 30.0) -> None:
        token = access_token or _resolve_token()
        if not token:
            raise RuntimeError("MAPILLARY_ACCESS_TOKEN is not set in .env")
        self._token = token
        self._client = httpx.Client(
            timeout=timeout,
            headers={"Authorization": f"OAuth {token}"},
        )
        # separate client for CDN downloads, no auth header (per PLAN section 3)
        self._cdn = httpx.Client(timeout=timeout)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> httpx.Response:
        client = kwargs.pop("_client", self._client)
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = client.request(method, url, **kwargs)
            except httpx.HTTPError as exc:
                last_exc = exc
                time.sleep(BACKOFF_BASE_S * (2**attempt))
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                ra = resp.headers.get("Retry-After")
                delay = (
                    float(ra)
                    if ra and ra.replace(".", "", 1).isdigit()
                    else BACKOFF_BASE_S * (2**attempt)
                )
                time.sleep(delay)
                continue

            if resp.status_code >= 400:
                # Mapillary error envelope: {"error": {"code": 4, "is_transient": true, ...}}
                try:
                    err = resp.json().get("error", {})
                except Exception:
                    err = {}
                if err.get("code") == 4 or err.get("is_transient") is True:
                    time.sleep(BACKOFF_BASE_S * (2**attempt))
                    continue
                resp.raise_for_status()

            return resp

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"exhausted {MAX_RETRIES} retries for {url}")

    def list_images_in_bbox(
        self,
        bbox: BBox,
        *,
        start_captured_at: datetime | None = None,
        end_captured_at: datetime | None = None,
        is_pano: bool | None = False,
        limit: int = 2000,
    ) -> list[ImageMeta]:
        bbox.assert_under_mapillary_limit()
        params: dict[str, str | int] = {
            "bbox": bbox.as_mapillary_str(),
            "fields": DEFAULT_FIELDS,
            "limit": limit,
        }
        if start_captured_at is not None:
            params["start_captured_at"] = _to_iso_z(start_captured_at)
        if end_captured_at is not None:
            params["end_captured_at"] = _to_iso_z(end_captured_at)

        resp = self._request_with_retry("GET", GRAPH_URL, params=params)
        payload = resp.json()
        rows = payload.get("data", []) or []

        out: list[ImageMeta] = []
        for r in rows:
            geom = r.get("computed_geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            lon, lat = coords[0], coords[1]
            if lon is None or lat is None:
                continue
            row_is_pano = bool(r.get("is_pano", False))
            # client-side is_pano filter, server-side is unreliable
            if is_pano is not None and row_is_pano != is_pano:
                continue
            seq = r.get("sequence")
            sequence_id = (
                seq
                if isinstance(seq, str)
                else (seq or {}).get("id")
                if isinstance(seq, dict)
                else None
            )
            captured_at = r.get("captured_at")
            if captured_at is None:
                continue
            url = r.get("thumb_2048_url")
            if not url:
                continue
            # Per Mapillary docs the Image entity exposes creator{id,username},
            # not a flat creator_id. Older sequence-search responses included
            # creator_id directly, so we tolerate both.
            creator_field = r.get("creator")
            if isinstance(creator_field, dict):
                creator_id_raw: object = creator_field.get("id")
            else:
                creator_id_raw = r.get("creator_id")
            out.append(
                ImageMeta(
                    image_id=str(r["id"]),
                    lon=float(lon),
                    lat=float(lat),
                    compass_angle=(
                        float(r["computed_compass_angle"])
                        if r.get("computed_compass_angle") is not None
                        else None
                    ),
                    captured_at_ms=int(captured_at),
                    camera_type=r.get("camera_type"),
                    is_pano=row_is_pano,
                    sequence_id=sequence_id,
                    creator_id=(int(creator_id_raw) if creator_id_raw is not None else None),
                    width=(int(r["width"]) if r.get("width") is not None else None),
                    height=(int(r["height"]) if r.get("height") is not None else None),
                    thumb_2048_url=url,
                )
            )
        return out

    def download_image(self, url: str) -> bytes:
        resp = self._request_with_retry("GET", url, _client=self._cdn)
        ctype = resp.headers.get("Content-Type", "")
        if not ctype.startswith("image/"):
            raise ValueError(f"expected image/* content-type, got {ctype!r} for {url}")
        return resp.content

    def close(self) -> None:
        self._client.close()
        self._cdn.close()

    def __enter__(self) -> MapillaryClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
