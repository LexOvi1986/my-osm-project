"""
Acceptance validation for per-city H3 urbanicity outputs (spec §E1).

Call ``validate_city_output`` after the pipeline completes for a city.
Raises ``UrbanicityValidationError`` listing all failed checks if any fail.
"""

from __future__ import annotations

import logging
from typing import List

import numpy as np
import geopandas as gpd

logger = logging.getLogger(__name__)


class UrbanicityValidationError(Exception):
    """Raised when one or more acceptance checks fail."""


def validate_city_output(gdf: gpd.GeoDataFrame, city_slug: str) -> None:
    """
    Run all E1 acceptance checks against a completed city GeoDataFrame.

    Checks
    ------
    E1.1  Non-empty hex set.
    E1.2  No negative values in density columns.
    E1.3  ``urbanicity_band_3_2_1`` values are in {1, 2, 3}.
    E1.4  ``urbanicity_score_continuous`` is finite for >99% of rows.
    E1.5  If signals were dropped, ``z_signal_density`` is all-NaN and
          effective signal weight is 0.
    E1.6  Reasonable coverage: intersection_density > 0 for ≥30% of hexes.
    E1.7  Reasonable coverage: road_density > 0 for ≥30% of hexes.

    Parameters
    ----------
    gdf:
        Completed city GeoDataFrame (as returned by ``run_city``).
    city_slug:
        City identifier used in log / error messages.

    Raises
    ------
    UrbanicityValidationError
        If any check fails.  All failures are collected and reported together.
    """
    failures: List[str] = []

    # E1.1 — Non-empty
    if len(gdf) == 0:
        failures.append("E1.1: Hex set is empty.")

    # E1.2 — No negative densities
    for col in [
        "intersection_density_per_km2",
        "road_density_km_per_km2",
        "signal_density_per_km2",
    ]:
        if col in gdf.columns:
            n_neg = int((gdf[col] < 0).sum())
            if n_neg > 0:
                failures.append(f"E1.2: {n_neg} negative value(s) in '{col}'.")

    # E1.3 — Band values in {1, 2, 3}
    band_col = "urbanicity_band_3_2_1"
    if band_col in gdf.columns:
        bad_bands = gdf[~gdf[band_col].isin([1, 2, 3])][band_col].unique()
        if len(bad_bands) > 0:
            failures.append(f"E1.3: Invalid band values found: {sorted(bad_bands)}.")

    # E1.4 — Score finiteness >99%
    score_col = "urbanicity_score_continuous"
    if score_col in gdf.columns:
        finite_pct = float(np.isfinite(gdf[score_col]).mean())
        if finite_pct < 0.99:
            failures.append(
                f"E1.4: Only {finite_pct:.1%} of scores are finite (need >99%)."
            )

    # E1.5 — Signal consistency
    signals_used = gdf.attrs.get("signals_used")
    z_sig_col = "z_signal_density"
    if signals_used is False:
        # z_signal_density must be all-NaN
        if z_sig_col in gdf.columns and gdf[z_sig_col].notna().any():
            failures.append(
                "E1.5: signals_used=False but z_signal_density has non-NaN values."
            )
        w_sig_eff = gdf.attrs.get("w_sig_eff", None)
        if w_sig_eff is not None and abs(w_sig_eff) > 1e-9:
            failures.append(
                f"E1.5: signals_used=False but w_sig_eff={w_sig_eff:.4f} (expected 0)."
            )
    elif signals_used is True:
        # z_signal_density must NOT be all-NaN
        if z_sig_col in gdf.columns and gdf[z_sig_col].isna().all():
            failures.append(
                "E1.5: signals_used=True but z_signal_density is all-NaN."
            )

    # E1.6 — Reasonable intersection coverage
    int_col = "intersection_density_per_km2"
    if int_col in gdf.columns and len(gdf) > 0:
        pct_pos = float((gdf[int_col] > 0).mean())
        if pct_pos < 0.30:
            failures.append(
                f"E1.6: Only {pct_pos:.1%} of hexes have intersection_density > 0 "
                f"(need ≥30%)."
            )

    # E1.7 — Reasonable road coverage
    road_col = "road_density_km_per_km2"
    if road_col in gdf.columns and len(gdf) > 0:
        pct_pos = float((gdf[road_col] > 0).mean())
        if pct_pos < 0.30:
            failures.append(
                f"E1.7: Only {pct_pos:.1%} of hexes have road_density > 0 "
                f"(need ≥30%)."
            )

    # Report
    if failures:
        msg = f"[{city_slug}] Validation failed ({len(failures)} issue(s)):\n" + "\n".join(
            f"  {f}" for f in failures
        )
        logger.error(msg)
        raise UrbanicityValidationError(msg)

    logger.info("[%s] All validation checks passed (%d hexes).", city_slug, len(gdf))
