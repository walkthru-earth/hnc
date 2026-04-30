"""TRIBE v2 inference wrapper, vision-only mode.

TRIBE v2 outputs are float32 z-scored fMRI BOLD predictions in arbitrary units,
on the fsaverage5 cortical mesh (20484 vertices). Values are NOT calibrated to
real BOLD signal magnitude. Predictions are offset by -5 s for hemodynamic lag.
License of the released checkpoint is CC-BY-NC-4.0.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

N_VERTICES: int = 20484
MESH: str = "fsaverage5"
TR_SECONDS: float = 1.0
HEMODYNAMIC_LAG_SECONDS: float = -5.0
MODEL_LICENSE: str = "CC-BY-NC-4.0"


class TribeRunner:
    def __init__(
        self,
        repo_id: str = "facebook/tribev2",
        cache_folder: str = "./tribe_cache",
        device: str = "auto",
    ) -> None:
        self.repo_id = repo_id
        self.cache_folder = cache_folder
        self.device = device
        self.model: Any | None = None

    def load(self) -> None:
        from tribev2 import TribeModel

        self.model = TribeModel.from_pretrained(
            self.repo_id,
            cache_folder=self.cache_folder,
            device=self.device,
            config_update={
                "average_subjects": True,
                "features_to_use": ["video"],
            },
        )

    def predict_clip(self, video_path: Path) -> tuple[np.ndarray, list]:
        if self.model is None:
            raise RuntimeError("TribeRunner.load() must be called before predict_clip()")
        df = self.model.get_events_dataframe(video_path=str(video_path))
        preds, segments = self.model.predict(events=df)
        preds = np.asarray(preds, dtype=np.float32)
        return preds, segments
