from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_ENV_LOADED = False


def _ensure_loaded() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=repo_root / ".env")
    _ENV_LOADED = True


def get_mapillary_token() -> str:
    _ensure_loaded()
    tok = os.environ.get("MAPILLARY_ACCESS_TOKEN")
    if not tok:
        raise RuntimeError("MAPILLARY_ACCESS_TOKEN not set in .env")
    return tok.strip()


def get_hf_token() -> str | None:
    _ensure_loaded()
    return os.environ.get("HF_TOKEN") or None


def get_hf_dataset_repo_id() -> str | None:
    _ensure_loaded()
    return os.environ.get("HF_DATASET_REPO_ID") or None
