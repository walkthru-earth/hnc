"""HCP MMP1 parcel summarization and functional-alias top-K helpers.

TRIBE v2 outputs are float32 z-scored fMRI BOLD predictions in arbitrary units
on the fsaverage5 cortical mesh. Parcel-mean and alias scores inherit those
arbitrary units, they are not calibrated to real BOLD signal magnitude. Use
them for relative ranking across regions and across images, not as absolute
activation amplitudes.
"""

from __future__ import annotations

import numpy as np

HCP_ALIAS_GLOBS: dict[str, list[str]] = {
    "FFA": ["FFC*"],
    "PPA": ["PHA1*", "PHA2*", "PHA3*"],
    "EBA": ["TE2p*", "PH*"],
    "VWFA": ["VVC*", "TE2p*"],
    "STSdp": ["STSdp*"],
    "STSva": ["STSva*"],
    "Broca45": ["45*"],
    "A5": ["A5*"],
    "MT": ["MT*", "MST*"],
    "V1": ["V1*"],
    "V4": ["V4*"],
    "TPJ": ["TPOJ1*"],
}


def parcel_means(preds_row: np.ndarray) -> dict[str, float]:
    from tribev2.utils import summarize_by_roi

    result = summarize_by_roi(preds_row)
    if isinstance(result, dict):
        return {str(k): float(v) for k, v in result.items()}

    arr = np.asarray(result)
    try:
        from tribev2.utils import get_hcp_labels

        labels = get_hcp_labels(mesh="fsaverage5")
        names = list(labels.keys())
    except Exception:
        names = [f"parcel_{i:03d}" for i in range(arr.shape[-1])]
    return {names[i]: float(arr[i]) for i in range(min(len(names), arr.shape[-1]))}


def alias_scores(
    preds_row: np.ndarray | dict[str, float],
    alias_map: dict[str, list[str]] = HCP_ALIAS_GLOBS,
) -> dict[str, float]:
    import fnmatch

    # Accept either a vertex vector (which we summarise) or a pre-computed
    # parcel-mean dict. Re-summarising a dict produces nonsense, and
    # tribev2.utils.summarize_by_roi rejects non-vertex shapes outright.
    if isinstance(preds_row, dict):
        means = {str(k): float(v) for k, v in preds_row.items()}
    else:
        means = parcel_means(preds_row)
    parcel_names = list(means.keys())
    out: dict[str, float] = {}
    for alias, globs in alias_map.items():
        matched: list[float] = []
        seen: set[str] = set()
        for pattern in globs:
            for name in parcel_names:
                if name in seen:
                    continue
                if fnmatch.fnmatchcase(name, pattern):
                    matched.append(means[name])
                    seen.add(name)
        out[alias] = float(np.mean(matched)) if matched else float("nan")
    return out


def top_k_aliases(preds_row: np.ndarray | dict[str, float], k: int = 8) -> list[dict]:
    scores = alias_scores(preds_row)
    pairs = [(name, score) for name, score in scores.items() if not np.isnan(score)]
    pairs.sort(key=lambda kv: kv[1], reverse=True)
    return [{"name": name, "score": float(score)} for name, score in pairs[:k]]


def collapse_segments(preds: np.ndarray, mode: str = "mean") -> np.ndarray:
    arr = np.asarray(preds)
    if arr.ndim == 1:
        return arr.astype(np.float32, copy=False)
    if mode == "mean":
        return arr.mean(axis=0).astype(np.float32, copy=False)
    if mode == "max":
        return arr.max(axis=0).astype(np.float32, copy=False)
    raise ValueError(f"unknown collapse mode: {mode!r}")
