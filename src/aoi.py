from __future__ import annotations

from dataclasses import dataclass
from math import cos, pi

MAPILLARY_BBOX_MAX_AREA_SQ_DEG: float = 0.01


@dataclass(frozen=True)
class BBox:
    west: float
    south: float
    east: float
    north: float

    def as_mapillary_str(self) -> str:
        return f"{self.west:.6f},{self.south:.6f},{self.east:.6f},{self.north:.6f}"

    def area_sq_deg(self) -> float:
        return (self.east - self.west) * (self.north - self.south)

    def assert_under_mapillary_limit(self) -> None:
        area = self.area_sq_deg()
        if area > MAPILLARY_BBOX_MAX_AREA_SQ_DEG:
            raise ValueError(
                f"bbox area {area:.6f} sq deg exceeds Mapillary limit "
                f"{MAPILLARY_BBOX_MAX_AREA_SQ_DEG} sq deg"
            )


def true_square_bbox(lon0: float, lat0: float, side_m: float) -> BBox:
    lat0_rad = lat0 * pi / 180.0
    dlat = side_m / 111320.0
    dlon = side_m / (111320.0 * cos(lat0_rad))
    return BBox(
        west=lon0 - dlon / 2.0,
        south=lat0 - dlat / 2.0,
        east=lon0 + dlon / 2.0,
        north=lat0 + dlat / 2.0,
    )


CAMDEN_TOWN: BBox = BBox(-0.146500, 51.540500, -0.142200, 51.543000)
BOROUGH_MARKET: BBox = BBox(-0.092500, 51.504300, -0.088200, 51.506800)
