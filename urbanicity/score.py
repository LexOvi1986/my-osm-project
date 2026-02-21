"""
Robust normalization, urbanicity score, and band discretization.

All normalization is computed within a single city so that scores reflect
relative urban intensity within that city (not cross-city comparisons).

Key design decisions
--------------------
* Robust z-score uses median + MAD (as specified).  When MAD ≈ 0 (degenerate
  case where >50% of hexes share the same value), the pipeline falls back to
  the population standard deviation; if that is also ~0 the metric is
  constant and z-scores are returned as 0.  Final z-scores are clamped to
  [-10, 10] so that a single extreme outlier cannot dominate the composite.
* Signal density is dropped (weights redistributed) when:
    signal_mode="off"  — always dropped, OR
    signal_mode="auto" — fewer than 5% of hexes contain ≥1 signal feature, OR
    signal_mode="on"   — never dropped.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd

from urbanicity.config import (
    FIELD_SEMANTICS,
    MAD_EPS,
    QUANTILE_HIGH,
    QUANTILE_LOW,
    SIGNAL_SPARSITY_THRESHOLD,
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
    Compute a robust z-score: Z(x) = (x − median) / (MAD + eps).

    Fallback when MAD ≈ 0: use population std.  If std is also ≈ 0 all
    values are identical and zeros are returned.  Final values are clamped
    to [-10, 10] to prevent extreme outliers from dominating the composite.

    Parameters
    ----------
    series:
        Numeric pd.Series.

    Returns
    -------
    pd.Series of robust z-scores, clamped to [-10, 10].
    """
    median = series.median()
    mad = (series - median).abs().median()

    if mad < MAD_EPS:
        scale = series.std(ddof=0)
        if scale < MAD_EPS:
            return pd.Series(0.0, index=series.index, name=series.name)
    else:
        scale = mad

    z = (series - median) / (scale + MAD_EPS)
    return z.clip(lower=-10.0, upper=10.0)


# ---------------------------------------------------------------------------
# Signal usability decision
# ---------------------------------------------------------------------------

def _signal_fraction(gdf: gpd.GeoDataFrame) -> float:
    """Return the fraction of hexes that contain ≥1 signal/stop feature."""
    return (gdf["signal_density_per_km2"] > 0).mean()


def _should_use_signals(
    gdf: gpd.GeoDataFrame,
    signal_mode: str,
    city: CityConfig,
) -> bool:
    """
    Decide whether to include signal density in the composite score.

    Parameters
    ----------
    gdf:
        Hex GeoDataFrame with ``signal_density_per_km2`` column.
    signal_mode:
        "on"   — always include signals.
        "off"  — always drop signals.
        "auto" — include only if ≥5% of hexes have ≥1 signal (default).
    city:
        For logging.

    Returns
    -------
    bool — True if signals should be included.
    """
    if signal_mode == "on":
        logger.info("[%s] signal_mode=on: signals always included.", city.slug)
        return True
    if signal_mode == "off":
        logger.info("[%s] signal_mode=off: signals always dropped.", city.slug)
        return False

    # auto: apply 5% sparsity rule
    frac = _signal_fraction(gdf)
    use = frac >= SIGNAL_SPARSITY_THRESHOLD
    logger.info(
        "[%s] signal_mode=auto: %.1f%% of hexes have ≥1 signal → %s.",
        city.slug,
        frac * 100,
        "INCLUDED" if use else "DROPPED (too sparse)",
    )
    return use


# ---------------------------------------------------------------------------
# Urbanicity score
# ---------------------------------------------------------------------------

def compute_urbanicity_score(
    gdf: gpd.GeoDataFrame,
    city: CityConfig,
    signal_mode: str = "auto",
    weights: Optional[Tuple[float, float, float]] = None,
) -> gpd.GeoDataFrame:
    """
    Add robust z-score columns and the composite UrbanicityScore.

    Parameters
    ----------
    gdf:
        GeoDataFrame with ``intersection_density_per_km2``,
        ``road_density_km_per_km2``, ``signal_density_per_km2``.
    city:
        For logging.
    signal_mode:
        "on" | "off" | "auto".  See ``_should_use_signals``.
    weights:
        Optional (w_int, w_road, w_sig) tuple.  Must sum to 1.0.
        Defaults to (0.50, 0.30, 0.20).

    Returns
    -------
    GeoDataFrame with additional columns:
    ``z_intersection_density``, ``z_road_density``,
    ``z_signal_density`` (NaN when dropped),
    ``urbanicity_score_continuous``.
    """
    result = gdf.copy()

    # Resolve base weights
    if weights is not None:
        w_int_base, w_road_base, w_sig_base = weights
    else:
        w_int_base, w_road_base, w_sig_base = W_INTERSECTION, W_ROAD, W_SIGNAL

    # Compute z-scores for all three metrics
    result["z_intersection_density"] = robust_zscore(
        result["intersection_density_per_km2"]
    )
    result["z_road_density"] = robust_zscore(result["road_density_km_per_km2"])
    result["z_signal_density"] = robust_zscore(result["signal_density_per_km2"])

    # Decide whether to use signals
    use_signal = _should_use_signals(result, signal_mode, city)

    if use_signal:
        w_int  = w_int_base
        w_road = w_road_base
        w_sig  = w_sig_base
        logger.info(
            "[%s] Score weights: int=%.3f  road=%.3f  sig=%.3f",
            city.slug, w_int, w_road, w_sig,
        )
    else:
        # Renormalize intersection + road weights to sum to 1
        total_no_sig = w_int_base + w_road_base
        w_int  = w_int_base  / total_no_sig
        w_road = w_road_base / total_no_sig
        w_sig  = 0.0
        result["z_signal_density"] = np.nan
        logger.info(
            "[%s] Score weights (signals dropped): int=%.3f  road=%.3f",
            city.slug, w_int, w_road,
        )

    result["urbanicity_score_continuous"] = (
        w_int  * result["z_intersection_density"]
        + w_road * result["z_road_density"]
        + w_sig  * result["z_signal_density"].fillna(0.0)
    )

    # Store effective weights for downstream reporting
    result.attrs["w_int_eff"]  = w_int
    result.attrs["w_road_eff"] = w_road
    result.attrs["w_sig_eff"]  = w_sig
    result.attrs["signals_used"] = use_signal

    logger.info(
        "[%s] urbanicity_score_continuous: min=%.4f  mean=%.4f  max=%.4f",
        city.slug,
        result["urbanicity_score_continuous"].min(),
        result["urbanicity_score_continuous"].mean(),
        result["urbanicity_score_continuous"].max(),
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
    Assign a discrete urbanicity band (3/2/1) and attach threshold columns.

    Thresholds are computed within the city on ``urbanicity_score_continuous``
    so that the band distribution always reflects relative urban intensity
    for that city.

    | Band | Label       | Condition                           |
    |------|-------------|-------------------------------------|
    |  3   | Very Urban  | score >= T_high = quantile(0.70)    |
    |  2   | Urban       | T_low < score < T_high              |
    |  1   | Suburban    | score <= T_low  = quantile(0.30)    |

    Columns added:
    ``urbanicity_band_3_2_1``, ``t_low_q30``, ``t_high_q70``.

    Parameters
    ----------
    gdf:
        GeoDataFrame with ``urbanicity_score_continuous``.
    city:
        For logging.
    q_high, q_low:
        Upper/lower quantile thresholds (defaults 0.70 / 0.30).

    Returns
    -------
    GeoDataFrame with band and threshold columns added.
    """
    result = gdf.copy()
    scores = result["urbanicity_score_continuous"]

    t_high = float(scores.quantile(q_high))
    t_low  = float(scores.quantile(q_low))

    logger.info(
        "[%s] Band thresholds: T_low=%.4f (q%.0f)  T_high=%.4f (q%.0f)",
        city.slug, t_low, q_low * 100, t_high, q_high * 100,
    )

    def _band(score: float) -> int:
        if score >= t_high:
            return 3
        if score <= t_low:
            return 1
        return 2

    result["urbanicity_band_3_2_1"] = scores.apply(_band).astype("int8")
    result["t_low_q30"]  = t_low
    result["t_high_q70"] = t_high

    band_counts = result["urbanicity_band_3_2_1"].value_counts().sort_index()
    logger.info(
        "[%s] Band distribution:\n%s",
        city.slug,
        band_counts.to_string(),
    )
    return result
