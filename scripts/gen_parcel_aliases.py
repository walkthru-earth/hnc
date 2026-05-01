"""Generate the static parcel atlas the website uses to spotlight functional
regions on the 3D cortex when a user clicks a row in "What this image triggers".

Output: parcel_aliases.json — { "<alias>": [<fsaverage5_vertex_index>, ...], ... }
where vertex indices are in the [0..20483] global fsaverage5 ordering
(left hemisphere first, right hemisphere offset by 10242), exactly as the
brain_activity vector in hnc_borough.parquet uses them.

Aliases come from `src/roi_summary.py::HCP_ALIAS_GLOBS`. We expand each glob
against the HCP MMP1 parcellation and union the per-parcel vertex sets.

Run once from the hnc repo root:

    uv run python scripts/gen_parcel_aliases.py \\
        --out ../website/walkthru-earth.github.io/public/hnc/parcel_aliases.json

The first run downloads only the fsaverage subject (~600 MB via
``mne.datasets.fetch_fsaverage``) plus the two HCP-MMP1 .annot files (~5 MB
via ``mne.datasets.fetch_hcp_mmp_parcellation``). It deliberately avoids
``mne.datasets.sample`` (1.65 GB), which the in-tree ``tribev2.utils``
helper would otherwise pull. After the initial fetch this is a few seconds.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import sys
from pathlib import Path

import mne  # type: ignore
import numpy as np  # type: ignore

# Re-uses the alias globs from the inference pipeline so the website is
# always in sync with what `roi_summary.alias_scores` actually scored.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.roi_summary import HCP_ALIAS_GLOBS  # noqa: E402

# fsaverage5 has 10242 vertices per hemisphere, 20484 total. The activity
# vector in our parquet is laid out left-then-right, so right-hemi indices
# get this offset added.
FSAV5_HEMI = 10242


def get_hcp_labels_fsav5() -> dict[str, np.ndarray]:
    """Replicates ``tribev2.utils.get_hcp_labels(mesh="fsaverage5", hemi="both")``
    without pulling MNE-sample (1.65 GB). Uses fetch_fsaverage instead.
    """
    fs_dir = mne.datasets.fetch_fsaverage(verbose=True)
    subjects_dir = Path(fs_dir).parent
    mne.datasets.fetch_hcp_mmp_parcellation(
        subjects_dir=subjects_dir, accept=True, combine=False, verbose=True
    )
    labels = mne.read_labels_from_annot(
        "fsaverage", "HCPMMP1", hemi="both", subjects_dir=subjects_dir
    )

    label_to_vertices: dict[str, np.ndarray] = {}
    for label in labels:
        name = label.name[2:]  # strip "L_" / "R_" prefix
        name = name.replace("_ROI", "")
        # Hemisphere is encoded in the label.hemi attribute.
        hemi = label.hemi  # "lh" or "rh"
        verts = np.asarray(label.vertices, dtype=np.int64)
        # Keep only vertices that survive the fsaverage5 down-sampling
        # (icosahedron subdivision keeps the lowest-index verts).
        verts = verts[verts < FSAV5_HEMI]
        if hemi == "rh":
            verts = verts + FSAV5_HEMI
        # Strip "-lh" / "-rh" suffix that mne adds, then merge by alias name.
        clean = name.replace("-rh", "").replace("-lh", "")
        if clean in label_to_vertices:
            label_to_vertices[clean] = np.concatenate(
                [label_to_vertices[clean], verts]
            )
        else:
            label_to_vertices[clean] = verts

    return label_to_vertices


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Path to write parcel_aliases.json (e.g. public/hnc/parcel_aliases.json).",
    )
    args = parser.parse_args()

    print("→ resolving HCP MMP1 parcels at fsaverage5…", flush=True)
    labels = get_hcp_labels_fsav5()
    parcel_names = list(labels.keys())
    print(f"  loaded {len(parcel_names)} parcels", flush=True)

    out: dict[str, list[int]] = {}
    for alias, globs in HCP_ALIAS_GLOBS.items():
        verts: set[int] = set()
        matched_parcels: list[str] = []
        for pattern in globs:
            for name in parcel_names:
                if fnmatch.fnmatchcase(name, pattern):
                    matched_parcels.append(name)
                    verts.update(int(v) for v in labels[name])
        out[alias] = sorted(verts)
        print(
            f"  {alias:8s} globs={globs!r:35s} parcels={len(matched_parcels):3d} verts={len(out[alias]):4d}",
            flush=True,
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, separators=(",", ":")))
    size_kb = args.out.stat().st_size / 1024
    print(f"\nwrote {args.out}  ({size_kb:.1f} KB, {len(out)} aliases)", flush=True)


if __name__ == "__main__":
    main()
