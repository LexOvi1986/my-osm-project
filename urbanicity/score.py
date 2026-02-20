"""
Robust normalization, urbanicity score, and band discretization.

All normalization is computed within a single city to make scores
comparable across hexes of the same city (not cross-city).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import geopandas as gpd

from urbanicity.config import (
    MAD_EPS,
    QUANTILE_HIGH,
    QUANTILE_LOW,
    W_INTERSECTION,
    W_INTERSECTION_NO_SIG,
    W_ROAD,
    W_ROAD_NO_SIG,
    W_SIGNAL,
    CityConfig,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Robust z-score
# ---------------------------------------------------------------------------

def robust_zscore(series: pd.Series) -> pd.Series:
    """
    Compute a robust z-score using median and MAD.

    Formula:
        Z(x) = (x - median(x)) / (MAD(x) + eps)

    A small epsilon (``MAD_EPS``) prevents division by zero when all values
    are identical (e.g., all-zero signal density in low-density hexes).

    Fallback when MAD ≈ 0 (e.g. >50% of hexes have zero signal density):
    use the series standard deviation as the scale parameter so that the
    denominator is never astronomically small.  If std is also zero, all
    values are identical and z-scores are returned as 0.

    The final result is clamped to [-10, 10] to prevent a single extreme
    outlier from dominating the composite score.

    Parameters
    ----------
    series:
        Numeric pandas Series.

    Returns
    -------
    pd.Series of the same shape with robust z-scores, clamped to [-10, 10].
    """
    median = series.median()
    mad = (series - median).abs().median()

    if mad < MAD_EPS:
        # MAD is effectively zero: fall back to standard deviation
        scale = series.std(ddof=0)
        if scale < MAD_EPS:
            # All values are identical — return zeros
            return pd.Series(0.0, index=series.index, name=series.name)
    else:
        scale = mad

    z = (series - median) / (scale + MAD_EPS)
    # Clamp extreme values so no single outlier dominates the composite
    return z.clip(lower=-10.0, upper=10.0)


# ---------------------------------------------------------------------------
# Urbanicity score
# ---------------------------------------------------------------------------

def compute_urbanicity_score(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
) -> gpd.GeoDataFrame:
    """
    Add z-score columns and the composite UrbanicityScore to *gdf*.

    Signal density is treated as "available" only when its total across the
    city exceeds zero. If unavailable, weights are redistributed to
    intersection and road density.

    Parameters
    ----------
    gdf:
        GeoDataFrame with columns:
        ``intersection_density_per_km2``,
        ``road_density_km_per_km2``,
        ``signal_density_per_km2``.
    city:
        For logging.

    Returns
    -------
    GeoDataFrame with additional columns:
    ``z_intersection_density``, ``z_road_density``, ``z_signal_density``,
    ``urbanicity_score``.
    """
    result = gdf.copy()

    # Compute robust z-scores
    result["z_intersection_density"] = robust_zscore(
        result["intersection_density_per_km2"]
    )
    result["z_road_density"] = robust_zscore(result["road_density_km_per_km2"])
    result["z_signal_density"] = robust_zscore(result["signal_density_per_km2"])

    # Determine if signal data is usable
    signal_total = result["signal_density_per_km2"].sum()
    use_signal = signal_total > 0.0

    if use_signal:
        w_int = W_INTERSECTION
        w_road = W_ROAD
        w_sig = W_SIGNAL
        logger.info(
            "[%s] Signal data available (total density sum=%.2f). "
            "Weights: int=%.3f road=%.3f sig=%.3f",
            city.slug, signal_total, w_int, w_road, w_sig,
        )
    else:
        w_int = W_INTERSECTION_NO_SIG
        w_road = W_ROAD_NO_SIG
        w_sig = 0.0
        logger.warning(
            "[%s] Signal data unavailable or all-zero. "
            "Redistributing weights: int=%.3f road=%.3f",
            city.slug, w_int, w_road,
        )
        result["z_signal_density"] = np.nan

    result["urbanicity_score"] = (
        w_int * result["z_intersection_density"]
        + w_road * result["z_road_density"]
        + w_sig * result["z_signal_density"].fillna(0.0)
    )

    logger.info(
        "[%s] Urbanicity score: min=%.4f, mean=%.4f, max=%.4f",
        city.slug,
        result["urbanicity_score"].min(),
        result["urbanicity_score"].mean(),
        result["urbanicity_score"].max(),
    )
    return result


# ---------------------------------------------------------------------------
# Band discretization
# ---------------------------------------------------------------------------

def assign_urbanicity_band(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    q_high: float = QUANTILE_HIGH,
    q_low: float = QUANTILE_LOW,
) -> gpd.GeoDataFrame:
    """
    Assign a discrete urbanicity band (3/2/1) based on score quantiles.

    Thresholds are computed within the city so that the band distribution
    always reflects relative urban intensity for that city.

    | Band | Label       | Condition                            |
    |------|-------------|--------------------------------------|
    |  3   | Very Urban  | score >= quantile(0.70)              |
    |  2   | Urban       | quantile(0.30) < score < q(0.70)     |
    |  1   | Suburban    | score <= quantile(0.30)              |

    Parameters
    ----------
    gdf:
        GeoDataFrame with ``urbanicity_score`` column.
    city:
        For logging.
    q_high, q_low:
        Upper and lower quantile thresholds.

    Returns
    -------
    GeoDataFrame with ``urbanicity_band_3_2_1`` column (int8).
    """
    result = gdf.copy()
    scores = result["urbanicity_score"]

    t_high = scores.quantile(q_high)
    t_low = scores.quantile(q_low)

    logger.info(
        "[%s] Band thresholds: T_low=%.4f (q%.0f), T_high=%.4f (q%.0f)",
        city.slug, t_low, q_low * 100, t_high, q_high * 100,
    )

    def _band(score: float) -> int:
        if score >= t_high:
            return 3
        if score <= t_low:
            return 1
        return 2

    result["urbanicity_band_3_2_1"] = scores.apply(_band).astype("int8")

    band_counts = result["urbanicity_band_3_2_1"].value_counts().sort_index()
    logger.info(
        "[%s] Band distribution:\n%s",
        city.slug,
        band_counts.to_string(),
    )
    return result
