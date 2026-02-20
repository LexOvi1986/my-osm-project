"""
Main pipeline orchestrator.

Ties together OSM download → H3 grid → metrics → scoring → output.
Called by the CLI (cli.py) and can also be imported directly.
"""

from __future__ import annotations

import logging

import geopandas as gpd

from urbanicity.config import (
    CityConfig,
    DEFAULT_BUFFER_M,
    DEFAULT_H3_RES,
    INTERSECTION_MIN_DEGREE,
)
from urbanicity.h3grid import (
    build_hex_geodataframe,
    get_city_boundary,
    get_graph_crs,
    polyfill_boundary,
)
from urbanicity.io import write_geojson, write_parquet, write_summary
from urbanicity.metrics import (
    assemble_metrics,
    compute_intersection_density,
    compute_road_density,
    compute_signal_density,
)
from urbanicity.osm import (
    compute_intersection_nodes,
    load_graph,
    load_nodes_edges,
    load_signals,
)
from urbanicity.score import assign_urbanicity_band, compute_urbanicity_score

logger = logging.getLogger(__name__)


def run_city(
    city: CityConfig,
    h3_res: int = DEFAULT_H3_RES,
    buffer_m: float = DEFAULT_BUFFER_M,
    write_geojson: bool = True,
    force: bool = False,
) -> gpd.GeoDataFrame:
    """
    Execute the full urbanicity pipeline for a single city.

    Parameters
    ----------
    city:
        CityConfig describing the city to process.
    h3_res:
        H3 resolution (default 8).
    buffer_m:
        Buffer in metres applied to the city boundary before polyfilling.
    write_geojson:
        Whether to write a GeoJSON output file in addition to Parquet.
    force:
        If True, ignore cached OSM data and re-download.

    Returns
    -------
    GeoDataFrame with all computed columns for the city.
    """
    logger.info("=" * 60)
    logger.info("[%s] Starting pipeline", city.name)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Download OSM drivable road graph
    # ------------------------------------------------------------------
    logger.info("[%s] Step 1 — Loading road graph…", city.slug)
    G = load_graph(city, force=force)
    graph_crs = get_graph_crs(G)
    logger.info("[%s] Graph CRS: %s", city.slug, graph_crs)

    # ------------------------------------------------------------------
    # Step 2: Nodes & edges GeoDataFrames
    # ------------------------------------------------------------------
    logger.info("[%s] Step 2 — Loading nodes/edges…", city.slug)
    nodes, edges = load_nodes_edges(city, G=G, force=force)

    # ------------------------------------------------------------------
    # Step 3: Intersection nodes (degree >= 3)
    # ------------------------------------------------------------------
    logger.info("[%s] Step 3 — Identifying intersection nodes…", city.slug)
    intersections = compute_intersection_nodes(
        city, G, nodes, min_degree=INTERSECTION_MIN_DEGREE
    )

    # ------------------------------------------------------------------
    # Step 4: Signal/stop features
    # ------------------------------------------------------------------
    logger.info("[%s] Step 4 — Loading signal features…", city.slug)
    signals_wgs84 = load_signals(city, force=force)
    # Reproject to graph CRS for consistent spatial operations
    if not signals_wgs84.empty:
        signals = signals_wgs84.to_crs(graph_crs)
    else:
        signals = signals_wgs84

    # ------------------------------------------------------------------
    # Step 5: City boundary + H3 polyfill
    # ------------------------------------------------------------------
    logger.info("[%s] Step 5 — Generating H3 hex grid (res=%d)…", city.slug, h3_res)
    boundary_wgs84 = get_city_boundary(city, nodes)
    h3_indices = polyfill_boundary(boundary_wgs84, h3_res=h3_res, buffer_m=buffer_m)
    logger.info("[%s] H3 polyfill: %d hexes", city.slug, len(h3_indices))

    hexes = build_hex_geodataframe(
        h3_indices=h3_indices,
        h3_res=h3_res,
        city=city,
        projected_crs=graph_crs,
    )

    # ------------------------------------------------------------------
    # Step 6: Per-hex metric computation
    # ------------------------------------------------------------------
    logger.info("[%s] Step 6 — Computing per-hex metrics…", city.slug)

    int_density = compute_intersection_density(hexes, intersections, city)
    road_density = compute_road_density(hexes, edges, city)
    sig_density = compute_signal_density(hexes, signals, city)

    hexes = assemble_metrics(hexes, int_density, road_density, sig_density)

    # ------------------------------------------------------------------
    # Step 7: Normalize + score + discretize
    # ------------------------------------------------------------------
    logger.info("[%s] Step 7 — Computing urbanicity scores…", city.slug)
    hexes = compute_urbanicity_score(hexes, city)
    hexes = assign_urbanicity_band(hexes, city)

    # ------------------------------------------------------------------
    # Step 8: Write outputs
    # ------------------------------------------------------------------
    logger.info("[%s] Step 8 — Writing outputs…", city.slug)
    write_parquet(hexes, city)
    write_summary(hexes, city)
    if write_geojson:
        from urbanicity.io import write_geojson as _write_geojson
        _write_geojson(hexes, city)

    logger.info("[%s] Pipeline complete.", city.name)
    return hexes
