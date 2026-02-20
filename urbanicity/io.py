"""
Output writers: Parquet, GeoJSON, and summary statistics.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from urbanicity.config import OUTPUT_DIR, OUTPUT_COLUMNS, CityConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _city_output_dir(city: CityConfig) -> Path:
    d = OUTPUT_DIR / city.slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _select_output_columns(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return a plain DataFrame with only the canonical output columns."""
    available = [c for c in OUTPUT_COLUMNS if c in gdf.columns]
    missing = set(OUTPUT_COLUMNS) - set(available)
    if missing:
        logger.warning("Output is missing columns: %s", missing)
    return gdf[available].copy()


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

def write_parquet(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    filename: str = "h3_urbanicity_res8.parquet",
) -> Path:
    """
    Write the hex metrics to a Parquet file (no geometry column).

    Parameters
    ----------
    gdf:
        GeoDataFrame with all metric and score columns.
    city:
        Determines output subdirectory.
    filename:
        Output filename.

    Returns
    -------
    Path to the written file.
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename
    df = _select_output_columns(gdf)
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
    Write the hex metrics to a GeoJSON file using the WGS-84 hex polygons.

    The ``geometry_wgs84`` column (stored during H3 grid construction) is
    used so the output is in standard lat/lon coordinates.

    Parameters
    ----------
    gdf:
        GeoDataFrame with ``geometry_wgs84`` column and all metric columns.
    city:
        Determines output subdirectory.
    filename:
        Output filename.

    Returns
    -------
    Path to the written file.
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename

    if "geometry_wgs84" not in gdf.columns:
        logger.warning(
            "[%s] 'geometry_wgs84' not found; skipping GeoJSON output.", city.slug
        )
        return path

    df = _select_output_columns(gdf)

    geo_gdf = gpd.GeoDataFrame(
        df,
        geometry=gdf["geometry_wgs84"].values,
        crs="EPSG:4326",
    )
    geo_gdf.to_file(path, driver="GeoJSON")
    logger.info("[%s] GeoJSON written: %s (%d features)", city.slug, path, len(geo_gdf))
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
    Write a JSON summary of the urbanicity metrics for the city.

    Includes:
    - Band distribution (counts and percentages)
    - Descriptive stats for each metric
    - Top-10 and bottom-10 hexes by urbanicity score

    Parameters
    ----------
    gdf:
        GeoDataFrame with all computed columns.
    city:
        Determines output subdirectory.
    filename:
        Output filename.

    Returns
    -------
    Path to the written file.
    """
    out_dir = _city_output_dir(city)
    path = out_dir / filename

    band_counts = gdf["urbanicity_band_3_2_1"].value_counts().sort_index()
    band_pct = (band_counts / len(gdf) * 100).round(1)

    metric_cols = [
        "intersection_density_per_km2",
        "road_density_km_per_km2",
        "signal_density_per_km2",
        "urbanicity_score",
    ]

    desc = {}
    for col in metric_cols:
        if col in gdf.columns:
            s = gdf[col].describe()
            desc[col] = {k: round(float(v), 6) for k, v in s.items()}

    top10 = (
        gdf.nlargest(10, "urbanicity_score")[["h3_index", "urbanicity_score", "urbanicity_band_3_2_1"]]
        .assign(urbanicity_score=lambda d: d["urbanicity_score"].round(4))
        .to_dict(orient="records")
    )
    bottom10 = (
        gdf.nsmallest(10, "urbanicity_score")[["h3_index", "urbanicity_score", "urbanicity_band_3_2_1"]]
        .assign(urbanicity_score=lambda d: d["urbanicity_score"].round(4))
        .to_dict(orient="records")
    )

    summary = {
        "city": city.name,
        "slug": city.slug,
        "total_hexes": int(len(gdf)),
        "band_distribution": {
            "counts": {int(k): int(v) for k, v in band_counts.items()},
            "percent": {int(k): float(v) for k, v in band_pct.items()},
        },
        "metric_stats": desc,
        "top10_by_score": top10,
        "bottom10_by_score": bottom10,
    }

    with open(path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info("[%s] Summary written: %s", city.slug, path)

    # Print band distribution to console
    logger.info(
        "[%s] Band distribution: band_1=%d (%.1f%%), band_2=%d (%.1f%%), band_3=%d (%.1f%%)",
        city.slug,
        band_counts.get(1, 0), band_pct.get(1, 0.0),
        band_counts.get(2, 0), band_pct.get(2, 0.0),
        band_counts.get(3, 0), band_pct.get(3, 0.0),
    )
    return path
