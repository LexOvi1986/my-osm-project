"""
Per-hex metric computation.

All spatial operations are performed in a metres-based projected CRS so that
areas (km²) and lengths (km) are correct.

Performance notes
-----------------
* Intersection density uses a spatial join (sjoin) — O(n log n).
* Road density uses GeoPandas overlay + apportioned clipping — O(edges × hexes)
  in the worst case but bounded in practice by spatial indexing.
* Signal density uses sjoin — O(n log n).
"""

from __future__ import annotations

import logging
import warnings
from typing import Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.errors import ShapelyDeprecationWarning

from urbanicity.config import CityConfig

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


# ---------------------------------------------------------------------------
# 1. Intersection density
# ---------------------------------------------------------------------------

def compute_intersection_density(
    hexes: gpd.GeoDataFrame,
    intersections: gpd.GeoDataFrame,
    city: CityConfig,
) -> pd.Series:
    """
    Count intersection nodes per hex and divide by hex area (km²).

    Parameters
    ----------
    hexes:
        GeoDataFrame of hex polygons in metres CRS.
    intersections:
        GeoDataFrame of intersection node points in the **same** metres CRS.
    city:
        For logging only.

    Returns
    -------
    pd.Series aligned to ``hexes.index`` with intersection density values.
    """
    logger.info("[%s] Computing intersection density…", city.slug)

    if intersections.empty:
        logger.warning("[%s] No intersection nodes found.", city.slug)
        return pd.Series(0.0, index=hexes.index, name="intersection_density_per_km2")

    # Ensure matching CRS
    if hexes.crs != intersections.crs:
        intersections = intersections.to_crs(hexes.crs)

    # Spatial join: for each intersection point find which hex it falls into
    joined = gpd.sjoin(
        intersections[["geometry"]],
        hexes[["geometry", "hex_area_km2"]],
        how="inner",
        predicate="within",
    )

    counts = joined.groupby("index_right").size().rename("intersection_count")
    hexes_merged = hexes[["hex_area_km2"]].join(counts, how="left")
    hexes_merged["intersection_count"] = hexes_merged["intersection_count"].fillna(0)
    density = hexes_merged["intersection_count"] / hexes_merged["hex_area_km2"]
    density.name = "intersection_density_per_km2"

    logger.info(
        "[%s] Intersection density: min=%.2f, mean=%.2f, max=%.2f (per km²)",
        city.slug,
        density.min(),
        density.mean(),
        density.max(),
    )
    return density


# ---------------------------------------------------------------------------
# 2. Road network density (apportioned)
# ---------------------------------------------------------------------------

def compute_road_density(
    hexes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
    city: CityConfig,
    chunk_size: int = 5_000,
) -> pd.Series:
    """
    Compute road length per hex (km) divided by hex area (km²).

    Edge lengths are **apportioned** by geometric intersection: if an edge
    crosses multiple hexes, each hex receives a fraction of the edge's length
    proportional to the length of the clipped segment.

    Strategy
    --------
    1. Clip edges to each hex via overlay (``gpd.overlay``).
    2. Recompute the clipped segment length in metres.
    3. Aggregate per hex.

    To keep memory usage manageable the edge set is processed in spatial
    chunks (by hex batch).

    Parameters
    ----------
    hexes:
        GeoDataFrame of hex polygons in metres CRS.
    edges:
        GeoDataFrame of road edges with ``length_m`` column, in metres CRS.
    city:
        For logging only.
    chunk_size:
        Number of hexes processed per overlay batch.

    Returns
    -------
    pd.Series aligned to ``hexes.index`` with road density values.
    """
    logger.info("[%s] Computing road network density (apportioned)…", city.slug)

    if edges.empty:
        logger.warning("[%s] No edges found.", city.slug)
        return pd.Series(0.0, index=hexes.index, name="road_density_km_per_km2")

    # Ensure matching CRS
    if hexes.crs != edges.crs:
        edges = edges.to_crs(hexes.crs)

    hexes_reset = hexes[["geometry", "hex_area_km2"]].reset_index(drop=False)
    hex_idx_col = hexes_reset.columns[0]  # original index column name

    # Build spatial index on edges once
    edges_sindex = edges.sindex

    road_length_km: dict[int, float] = {}  # hex positional index → km

    n_hexes = len(hexes_reset)
    n_batches = max(1, (n_hexes + chunk_size - 1) // chunk_size)

    for batch_i in range(n_batches):
        start = batch_i * chunk_size
        end = min(start + chunk_size, n_hexes)
        hex_batch = hexes_reset.iloc[start:end]

        # Find candidate edges that intersect this batch's bounding box
        batch_union_bounds = hex_batch.geometry.total_bounds  # (minx, miny, maxx, maxy)
        candidate_idx = list(
            edges_sindex.intersection(batch_union_bounds)
        )
        if not candidate_idx:
            continue
        candidate_edges = edges.iloc[candidate_idx]

        # Overlay: intersect candidate edges with hex batch polygons
        try:
            clipped = gpd.overlay(
                candidate_edges[["geometry"]].reset_index(drop=True),
                hex_batch[[hex_idx_col, "geometry"]].reset_index(drop=True),
                how="intersection",
                keep_geom_type=False,
            )
        except Exception as exc:
            logger.debug(
                "[%s] Overlay error in batch %d/%d: %s — skipping.",
                city.slug, batch_i + 1, n_batches, exc,
            )
            continue

        if clipped.empty:
            continue

        # Drop non-linear geometry types (points from touching boundaries)
        clipped = clipped[
            clipped.geometry.geom_type.isin(["LineString", "MultiLineString"])
        ]
        if clipped.empty:
            continue

        # Recompute clipped segment length in metres (in projected CRS)
        clipped["clipped_length_m"] = clipped.geometry.length

        # Aggregate per hex (identified by the hex_idx_col which holds the
        # positional index into hexes_reset)
        agg = clipped.groupby(hex_idx_col)["clipped_length_m"].sum()

        for pos_idx, length_m in agg.items():
            road_length_km[int(pos_idx)] = road_length_km.get(int(pos_idx), 0.0) + length_m / 1_000

        if (batch_i + 1) % 10 == 0 or (batch_i + 1) == n_batches:
            logger.info(
                "[%s] Road density: processed batch %d/%d…",
                city.slug,
                batch_i + 1,
                n_batches,
            )

    # Map positional indices back to original hex index
    road_km_series = pd.Series(road_length_km, name="road_length_km")
    hexes_reset["road_length_km"] = hexes_reset.index.map(road_km_series).fillna(0.0)
    hexes_reset["road_density_km_per_km2"] = (
        hexes_reset["road_length_km"] / hexes_reset["hex_area_km2"]
    )

    # Restore original index alignment
    result = hexes_reset["road_density_km_per_km2"]
    result.index = hexes.index
    result.name = "road_density_km_per_km2"

    logger.info(
        "[%s] Road density: min=%.2f, mean=%.2f, max=%.2f (km/km²)",
        city.slug,
        result.min(),
        result.mean(),
        result.max(),
    )
    return result


# ---------------------------------------------------------------------------
# 3. Signal density
# ---------------------------------------------------------------------------

def compute_signal_density(
    hexes: gpd.GeoDataFrame,
    signals: gpd.GeoDataFrame,
    city: CityConfig,
) -> pd.Series:
    """
    Count signal/stop features per hex and divide by hex area (km²).

    Parameters
    ----------
    hexes:
        GeoDataFrame of hex polygons in metres CRS.
    signals:
        GeoDataFrame of point features (signals/stops) in any CRS.
    city:
        For logging only.

    Returns
    -------
    pd.Series aligned to ``hexes.index`` with signal density values.
    """
    logger.info("[%s] Computing signal density…", city.slug)

    if signals.empty:
        logger.warning("[%s] No signal features; signal density set to 0.", city.slug)
        return pd.Series(0.0, index=hexes.index, name="signal_density_per_km2")

    # Reproject to match hexes
    if signals.crs != hexes.crs:
        signals = signals.to_crs(hexes.crs)

    # Keep only point geometries
    signals = signals[signals.geometry.geom_type == "Point"].copy()
    if signals.empty:
        return pd.Series(0.0, index=hexes.index, name="signal_density_per_km2")

    joined = gpd.sjoin(
        signals[["geometry"]],
        hexes[["geometry", "hex_area_km2"]],
        how="inner",
        predicate="within",
    )

    counts = joined.groupby("index_right").size().rename("signal_count")
    hexes_merged = hexes[["hex_area_km2"]].join(counts, how="left")
    hexes_merged["signal_count"] = hexes_merged["signal_count"].fillna(0)
    density = hexes_merged["signal_count"] / hexes_merged["hex_area_km2"]
    density.name = "signal_density_per_km2"

    logger.info(
        "[%s] Signal density: min=%.4f, mean=%.4f, max=%.4f (per km²)",
        city.slug,
        density.min(),
        density.mean(),
        density.max(),
    )
    return density


# ---------------------------------------------------------------------------
# Composite assembly
# ---------------------------------------------------------------------------

def assemble_metrics(
    hexes: gpd.GeoDataFrame,
    intersection_density: pd.Series,
    road_density: pd.Series,
    signal_density: pd.Series,
) -> gpd.GeoDataFrame:
    """
    Attach metric columns to the hex GeoDataFrame.

    Parameters
    ----------
    hexes:
        Hex GeoDataFrame (projected CRS).
    intersection_density, road_density, signal_density:
        pd.Series aligned to ``hexes.index``.

    Returns
    -------
    GeoDataFrame with three new metric columns.
    """
    result = hexes.copy()
    result["intersection_density_per_km2"] = intersection_density.reindex(result.index).fillna(0.0)
    result["road_density_km_per_km2"] = road_density.reindex(result.index).fillna(0.0)
    result["signal_density_per_km2"] = signal_density.reindex(result.index).fillna(0.0)
    return result
