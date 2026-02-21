"""
Output writers: Parquet, GeoJSON, summary statistics, and thresholds sidecar.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

import geopandas as gpd
import pandas as pd

from urbanicity.config import OUTPUT_DIR, OUTPUT_COLUMNS, CityConfig

logger = logging.getLogger(__name__)

# Score column name (centralised so renames propagate automatically)
_SCORE_COL = "urbanicity_score_continuous"


class _SafeEncoder(json.JSONEncoder):
    """Convert numpy scalars and Python bools to plain JSON-serialisable types."""
    def default(self, obj):
        if isinstance(obj, bool):
            return bool(obj)
        if hasattr(obj, "item"):   # numpy scalar (int64, float64, bool_, …)
            return obj.item()
        return super().default(obj)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _city_output_dir(city: CityConfig) -> Path:
    d = OUTPUT_DIR / city.slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _select_output_columns(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return a plain DataFrame with the canonical output columns (in order)."""
    available = [c for c in OUTPUT_COLUMNS if c in gdf.columns]
    missing = set(OUTPUT_COLUMNS) - set(available)
    if missing:
        logger.warning("Output is missing columns: %s", sorted(missing))
    return gdf[available].copy()


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

def write_parquet(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    filename: str = "h3_urbanicity_res8.parquet",
) -> Path:
    """Write hex metrics to Parquet (no geometry column)."""
    out_dir = _city_output_dir(city)
    path = out_dir / filename
    df = _select_output_columns(gdf)
    # pandas serialises df.attrs as JSON Parquet metadata; clear it to avoid
    # TypeError with bool/float values on Python 3.9's json encoder.
    df.attrs = {}
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("[%s] Parquet written: %s (%d rows)", city.slug, path, len(df))
    return path


# ---------------------------------------------------------------------------
# GeoJSON
# ---------------------------------------------------------------------------

def write_geojson(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    filename: str = "h3_urbanicity_res8.geojson",
) -> Path:
    """
    Write hex metrics to GeoJSON using WGS-84 hex polygons.

    The ``geometry_wgs84`` column (stored during H3 grid construction) is
    used so the file is in standard lat/lon coordinates.
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename

    if "geometry_wgs84" not in gdf.columns:
        logger.warning("[%s] 'geometry_wgs84' not found; skipping GeoJSON.", city.slug)
        return path

    df = _select_output_columns(gdf)
    geo_gdf = gpd.GeoDataFrame(df, geometry=gdf["geometry_wgs84"].values, crs="EPSG:4326")
    geo_gdf.to_file(path, driver="GeoJSON")
    logger.info("[%s] GeoJSON written: %s (%d features)", city.slug, path, len(geo_gdf))
    return path


# ---------------------------------------------------------------------------
# Thresholds sidecar
# ---------------------------------------------------------------------------

def write_thresholds(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    h3_res: int,
    filename: str = "thresholds.json",
) -> Path:
    """
    Write a small JSON sidecar with per-city band thresholds and weights.

    Schema
    ------
    {
        "city": "Boston",
        "h3_res": 8,
        "t_low_q30": -0.8000,
        "t_high_q70": 7.4385,
        "signals_used": true,
        "weights_effective": {"w_intersection": 0.5, "w_road": 0.3, "w_signal": 0.2},
        "field_semantics": "DERIVED_FROM_OSM"
    }
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename

    t_low  = float(gdf["t_low_q30"].iloc[0])  if "t_low_q30"  in gdf.columns else None
    t_high = float(gdf["t_high_q70"].iloc[0]) if "t_high_q70" in gdf.columns else None

    signals_used = gdf.attrs.get("signals_used", None)
    w_int  = gdf.attrs.get("w_int_eff",  None)
    w_road = gdf.attrs.get("w_road_eff", None)
    w_sig  = gdf.attrs.get("w_sig_eff",  None)

    payload: Dict[str, Any] = {
        "city": city.name,
        "slug": city.slug,
        "h3_res": h3_res,
        "t_low_q30":  round(t_low,  6) if t_low  is not None else None,
        "t_high_q70": round(t_high, 6) if t_high is not None else None,
        "signals_used": signals_used,
        "weights_effective": {
            "w_intersection": round(w_int,  4) if w_int  is not None else None,
            "w_road":         round(w_road, 4) if w_road is not None else None,
            "w_signal":       round(w_sig,  4) if w_sig  is not None else None,
        },
        "field_semantics": gdf["field_semantics"].iloc[0] if "field_semantics" in gdf.columns else "DERIVED_FROM_OSM",
    }

    with open(path, "w") as f:
        json.dump(payload, f, indent=2, cls=_SafeEncoder)

    logger.info("[%s] Thresholds written: %s", city.slug, path)
    return path


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def write_summary(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    filename: str = "summary.json",
) -> Path:
    """
    Write a JSON summary: band distribution, metric stats, top/bottom hexes.
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename

    band_counts = gdf["urbanicity_band_3_2_1"].value_counts().sort_index()
    band_pct = (band_counts / len(gdf) * 100).round(1)

    metric_cols = [
        "intersection_density_per_km2",
        "road_density_km_per_km2",
        "signal_density_per_km2",
        _SCORE_COL,
    ]

    desc: Dict[str, Any] = {}
    for col in metric_cols:
        if col in gdf.columns:
            s = gdf[col].describe()
            desc[col] = {k: round(float(v), 6) for k, v in s.items()}

    top10 = (
        gdf.nlargest(10, _SCORE_COL)[[
            "h3_index", _SCORE_COL, "urbanicity_band_3_2_1"
        ]]
        .assign(**{_SCORE_COL: lambda d: d[_SCORE_COL].round(4)})
        .to_dict(orient="records")
    )
    bottom10 = (
        gdf.nsmallest(10, _SCORE_COL)[[
            "h3_index", _SCORE_COL, "urbanicity_band_3_2_1"
        ]]
        .assign(**{_SCORE_COL: lambda d: d[_SCORE_COL].round(4)})
        .to_dict(orient="records")
    )

    summary: Dict[str, Any] = {
        "city": city.name,
        "slug": city.slug,
        "total_hexes": int(len(gdf)),
        "signals_used": gdf.attrs.get("signals_used"),
        "band_distribution": {
            "counts":  {int(k): int(v)   for k, v in band_counts.items()},
            "percent": {int(k): float(v) for k, v in band_pct.items()},
        },
        "metric_stats": desc,
        "top10_by_score":    top10,
        "bottom10_by_score": bottom10,
    }

    with open(path, "w") as f:
        json.dump(summary, f, indent=2, cls=_SafeEncoder)

    logger.info("[%s] Summary written: %s", city.slug, path)
    logger.info(
        "[%s] Band distribution: band_1=%d (%.1f%%)  band_2=%d (%.1f%%)  band_3=%d (%.1f%%)",
        city.slug,
        band_counts.get(1, 0), band_pct.get(1, 0.0),
        band_counts.get(2, 0), band_pct.get(2, 0.0),
        band_counts.get(3, 0), band_pct.get(3, 0.0),
    )
    return path
