"""TRIBE v2 inference wrapper, vision-only mode.

TRIBE v2 outputs are float32 z-scored fMRI BOLD predictions in arbitrary units,
on the fsaverage5 cortical mesh (20484 vertices). Values are NOT calibrated to
real BOLD signal magnitude. Predictions are offset by -5 s for hemodynamic lag.
License of the released checkpoint is CC-BY-NC-4.0.
"""

from __future__ import annotations

import logging
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger("tribe_runner")

N_VERTICES: int = 20484
MESH: str = "fsaverage5"
TR_SECONDS: float = 1.0
HEMODYNAMIC_LAG_SECONDS: float = -5.0
MODEL_LICENSE: str = "CC-BY-NC-4.0"


def _detect_device(requested: str) -> str:
    if requested != "auto":
        return requested
    import torch

    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def _gpu_total_gb() -> float:
    import torch

    if not torch.cuda.is_available():
        return 0.0
    props = torch.cuda.get_device_properties(0)
    return float(props.total_memory) / (1024**3)


def auto_batch_size(device: str) -> int:
    """Pick a TRIBE batch_size from device VRAM.

    V-JEPA2-Giant resident weights are ~14 GB. Each 30-s segment activation
    at ViT-G adds ~1.5-2 GB on the forward pass. Sizing leaves ~4 GB headroom
    for fragmentation and the TRIBE head.
    """
    if device != "cuda":
        return 1
    gb = _gpu_total_gb()
    if gb >= 75:        # A100 80, H100 80
        return 24
    if gb >= 40:        # A100 40, A6000 48, L40S 48
        return 12
    if gb >= 22:        # L4 24, RTX 4090 24, RTX 3090 24
        return 4
    if gb >= 14:        # T4 16, V100 16
        return 1
    return 1


def auto_num_workers(device: str) -> int:
    if device == "cuda":
        return 4
    return 0


class TribeRunner:
    def __init__(
        self,
        repo_id: str = "facebook/tribev2",
        cache_folder: str = "./tribe_cache",
        device: str = "auto",
        batch_size: int | None = None,
        num_workers: int | None = None,
        autocast_dtype: str | None = "bfloat16",
    ) -> None:
        self.repo_id = repo_id
        self.cache_folder = cache_folder
        self.device = _detect_device(device)
        self.batch_size = batch_size if batch_size is not None else auto_batch_size(self.device)
        self.num_workers = (
            num_workers if num_workers is not None else auto_num_workers(self.device)
        )
        # bf16 is safe on Ampere+ (sm_80+). On older cards autocast_dtype should
        # be set to None or "float16" by the caller. Leave nullcontext on cpu/mps.
        self.autocast_dtype = autocast_dtype
        self.model: Any | None = None

    def _autocast(self):
        if self.device != "cuda" or not self.autocast_dtype:
            return nullcontext()
        import torch

        dtype = {
            "bfloat16": torch.bfloat16,
            "float16": torch.float16,
        }.get(self.autocast_dtype)
        if dtype is None:
            return nullcontext()
        return torch.autocast(device_type="cuda", dtype=dtype)

    def load(self) -> None:
        from tribev2 import TribeModel

        self.model = TribeModel.from_pretrained(
            self.repo_id,
            cache_folder=self.cache_folder,
            device=self.device,
        )
        # TRIBE v2 was trained on naturalistic video with speech. The default
        # predict() drops segments whose ns_events list is empty, which would
        # wipe out predictions for our silent static clips. Force-keep every
        # TR so visual-cortex activations survive even when no words are
        # transcribed.
        self.model.remove_empty_segments = False
        # Push our adaptive batch + workers into the internal DataLoader. The
        # default predict() builds get_loaders() with these knobs.
        self.model.data.batch_size = self.batch_size
        self.model.data.num_workers = self.num_workers
        logger.info(
            "tribe loaded device=%s batch_size=%d num_workers=%d autocast=%s vram_total=%.1fGB",
            self.device,
            self.batch_size,
            self.num_workers,
            self.autocast_dtype if self.device == "cuda" else "n/a",
            _gpu_total_gb() if self.device == "cuda" else 0.0,
        )

    def predict_clip(self, video_path: Path) -> tuple[np.ndarray, list]:
        """Single-clip predict, kept for tests and debugging."""
        if self.model is None:
            raise RuntimeError("TribeRunner.load() must be called before predict_clip()")
        df = self.model.get_events_dataframe(video_path=str(video_path))
        with self._autocast():
            preds, segments = self.model.predict(events=df)
        preds = np.asarray(preds, dtype=np.float32)
        return preds, segments

    def predict_clips(
        self, items: list[tuple[str, Path]]
    ) -> dict[str, np.ndarray]:
        """Batched predict over many clips in a single forward pass.

        items: list of (image_id, video_path). Each clip becomes one row in
        the events DataFrame with a unique timeline so segments don't cross
        clip boundaries.

        Returns a dict mapping image_id -> stacked predictions of shape
        (n_segments_for_clip, n_vertices). The caller collapses across the
        time axis if it only wants one vector per image.
        """
        if self.model is None:
            raise RuntimeError("TribeRunner.load() must be called before predict_clips()")
        if not items:
            return {}

        import pandas as pd

        rows = []
        for image_id, video_path in items:
            df = self.model.get_events_dataframe(video_path=str(video_path))
            df = df.copy()
            df["timeline"] = f"clip_{image_id}"
            df["subject"] = "default"
            rows.append(df)
        events = pd.concat(rows, ignore_index=True)

        with self._autocast():
            preds, segments = self.model.predict(events=events)
        preds = np.asarray(preds, dtype=np.float32)

        # Group rows back per clip via segment.timeline.
        out: dict[str, list[np.ndarray]] = {image_id: [] for image_id, _ in items}
        # Map timeline -> image_id. clip_{image_id} format inverts cleanly.
        timeline_to_id = {f"clip_{iid}": iid for iid, _ in items}
        for i, seg in enumerate(segments):
            iid = timeline_to_id.get(getattr(seg, "timeline", None))
            if iid is None:
                continue
            out[iid].append(preds[i])
        stacked: dict[str, np.ndarray] = {}
        for iid, vecs in out.items():
            if vecs:
                stacked[iid] = np.stack(vecs, axis=0)
        return stacked
