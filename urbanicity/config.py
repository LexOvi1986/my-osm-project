"""
Configuration: constants, city definitions, weights, thresholds, paths.
"""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR: Path = Path(__file__).resolve().parent.parent
CACHE_DIR: Path = ROOT_DIR / "cache"
OUTPUT_DIR: Path = ROOT_DIR / "outputs"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# H3 settings
# ---------------------------------------------------------------------------

DEFAULT_H3_RES: int = 8

# ---------------------------------------------------------------------------
# OSM network download settings
# ---------------------------------------------------------------------------

NETWORK_TYPE: str = "drive"

# Buffer (metres) applied to the city boundary polygon before polyfilling
# to ensure edge hexes that partially overlap the boundary are included.
DEFAULT_BUFFER_M: float = 300.0

# ---------------------------------------------------------------------------
# City definitions
# ---------------------------------------------------------------------------

@dataclass
class CityConfig:
    name: str          # human-readable display name
    slug: str          # filesystem-safe identifier (underscores)
    osm_query: str     # OSMnx geocode query string

CITIES: Dict[str, CityConfig] = {
    "seattle": CityConfig(
        name="Seattle",
        slug="seattle",
        osm_query="Seattle, Washington, USA",
    ),
    "los_angeles": CityConfig(
        name="Los Angeles",
        slug="los_angeles",
        osm_query="Los Angeles, California, USA",
    ),
    "austin": CityConfig(
        name="Austin",
        slug="austin",
        osm_query="Austin, Texas, USA",
    ),
    "chicago": CityConfig(
        name="Chicago",
        slug="chicago",
        osm_query="Chicago, Illinois, USA",
    ),
    "boston": CityConfig(
        name="Boston",
        slug="boston",
        osm_query="Boston, Massachusetts, USA",
    ),
}

ALL_CITY_SLUGS: List[str] = list(CITIES.keys())

# ---------------------------------------------------------------------------
# OSM signal/control tags to query
# ---------------------------------------------------------------------------

SIGNAL_TAGS: Dict[str, List[str]] = {
    "highway": ["traffic_signals", "stop"],
    "crossing": ["traffic_signals"],
}

# ---------------------------------------------------------------------------
# Intersection definition
# ---------------------------------------------------------------------------

INTERSECTION_MIN_DEGREE: int = 3  # node degree >= this → intersection node

# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

# Small epsilon to guard against division by zero in robust z-score
MAD_EPS: float = 1e-9

# ---------------------------------------------------------------------------
# Signal sparsity rule
# ---------------------------------------------------------------------------

# If fewer than this fraction of hexes in a city contain ≥1 signal/stop
# feature, signals are considered too sparse and dropped from the score.
SIGNAL_SPARSITY_THRESHOLD: float = 0.05  # 5 %

# Signal mode: "auto" (apply sparsity rule), "on" (always use), "off" (never use)
DEFAULT_SIGNAL_MODE: str = "auto"

# ---------------------------------------------------------------------------
# Urbanicity score weights
# ---------------------------------------------------------------------------

# Default weights when signals are included
DEFAULT_WEIGHTS: Tuple[float, float, float] = (0.50, 0.30, 0.20)
W_INTERSECTION: float = 0.50
W_ROAD: float = 0.30
W_SIGNAL: float = 0.20

# Fallback weights when signals are dropped (renormalized to sum=1)
W_INTERSECTION_NO_SIG: float = 0.625
W_ROAD_NO_SIG: float = 0.375

# ---------------------------------------------------------------------------
# Band discretization quantiles
# ---------------------------------------------------------------------------

QUANTILE_HIGH: float = 0.70   # score >= T_high → band 3 (Very Urban)
QUANTILE_LOW: float  = 0.30   # score <= T_low  → band 1 (Suburban)
# between → band 2 (Urban)

# ---------------------------------------------------------------------------
# Output field semantics marker
# ---------------------------------------------------------------------------

FIELD_SEMANTICS: str = "DERIVED_FROM_OSM"

# ---------------------------------------------------------------------------
# Output column schema (ordered, matches spec G exactly)
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS: List[str] = [
    "city",
    "h3_res",
    "h3_index",
    "hex_centroid_lat",
    "hex_centroid_lon",
    "hex_area_km2",
    "intersection_density_per_km2",
    "road_density_km_per_km2",
    "signal_density_per_km2",
    "z_intersection_density",
    "z_road_density",
    "z_signal_density",
    "urbanicity_score_continuous",
    "urbanicity_band_3_2_1",
    "t_low_q30",
    "t_high_q70",
    "field_semantics",
]
