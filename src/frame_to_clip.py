"""JPEG still to MP4 clip helpers for TRIBE v2 vision-only inference."""

from __future__ import annotations

from pathlib import Path


def jpeg_to_static_clip(
    jpeg_bytes: bytes,
    out_path: Path,
    *,
    n_frames: int = 480,
    fps: int = 16,
) -> Path:
    # 480 frames at 16 fps == 30 s, matching TRIBE v2's video chunker
    # (ChunkEvents min_duration=30 s, max_duration=60 s). Shorter clips
    # are dropped or merged by upstream and produce zero predictions.
    import io

    import imageio
    import numpy as np
    from PIL import Image

    img = Image.open(io.BytesIO(jpeg_bytes)).convert("RGB")
    frame = np.asarray(img, dtype=np.uint8)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    writer = imageio.get_writer(
        str(out_path),
        fps=fps,
        codec="libx264",
        pixelformat="yuv420p",
        macro_block_size=1,
    )
    try:
        for _ in range(n_frames):
            writer.append_data(frame)
    finally:
        writer.close()
    return out_path


def jpegs_to_pseudo_clip(
    jpeg_bytes_in_order: list[bytes],
    out_path: Path,
    *,
    fps: int = 16,
    target_n_frames: int = 64,
) -> Path:
    import io

    import imageio
    import numpy as np
    from PIL import Image

    if not jpeg_bytes_in_order:
        raise ValueError("jpeg_bytes_in_order must contain at least one JPEG")

    frames: list[np.ndarray] = []
    for jb in jpeg_bytes_in_order:
        img = Image.open(io.BytesIO(jb)).convert("RGB")
        frames.append(np.asarray(img, dtype=np.uint8))

    n = len(frames)
    if n >= target_n_frames:
        sequence = frames[:target_n_frames]
    else:
        # repeat each frame ceil(target/n) times then truncate, preserving temporal order
        repeats = (target_n_frames + n - 1) // n
        expanded: list[np.ndarray] = []
        for f in frames:
            expanded.extend([f] * repeats)
        sequence = expanded[:target_n_frames]

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    writer = imageio.get_writer(
        str(out_path),
        fps=fps,
        codec="libx264",
        pixelformat="yuv420p",
        macro_block_size=1,
    )
    try:
        for frame in sequence:
            writer.append_data(frame)
    finally:
        writer.close()
    return out_path
