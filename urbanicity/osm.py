"""
OSM data acquisition: street networks and point features.

Downloads are cached to disk as GraphML (for graphs) and GeoPackage/Parquet
(for point features) so that subsequent runs skip the network round-trip.
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import networkx as nx
import osmnx as ox
import pandas as pd
from shapely.geometry import Point

from urbanicity.config import (
    CACHE_DIR,
    NETWORK_TYPE,
    SIGNAL_TAGS,
    CityConfig,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _graph_cache_path(city: CityConfig) -> Path:
    return CACHE_DIR / f"{city.slug}_graph.graphml"


def _nodes_cache_path(city: CityConfig) -> Path:
    return CACHE_DIR / f"{city.slug}_nodes.parquet"


def _edges_cache_path(city: CityConfig) -> Path:
    return CACHE_DIR / f"{city.slug}_edges.parquet"


def _signals_cache_path(city: CityConfig) -> Path:
    return CACHE_DIR / f"{city.slug}_signals.parquet"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_graph(city: CityConfig, force: bool = False) -> nx.MultiDiGraph:
    """
    Return a projected drivable road graph for *city*.

    The graph is cached as GraphML. On subsequent calls the file is read
    directly without hitting the OSM Overpass API.

    Parameters
    ----------
    city:
        CityConfig identifying the city to fetch.
    force:
        If True, re-download even if a cache file exists.

    Returns
    -------
    nx.MultiDiGraph
        OSMnx graph projected to an appropriate meters-based UTM CRS.
    """
    cache_path = _graph_cache_path(city)

    if cache_path.exists() and not force:
        logger.info("[%s] Loading graph from cache: %s", city.slug, cache_path)
        G = ox.load_graphml(cache_path)
        # Ensure the graph has been projected (may already be from a previous run)
        crs_val = G.graph.get("crs", "")
        is_wgs84 = str(crs_val).upper() in ("EPSG:4326", "WGS 84", "") or "crs" not in G.graph
        if is_wgs84:
            logger.info("[%s] Projecting cached graph…", city.slug)
            G = ox.project_graph(G)
            ox.save_graphml(G, cache_path)
        return G

    logger.info("[%s] Downloading OSM graph (%s)…", city.slug, city.osm_query)
    G = ox.graph_from_place(city.osm_query, network_type=NETWORK_TYPE)
    logger.info("[%s] Projecting graph to UTM…", city.slug)
    G = ox.project_graph(G)
    ox.save_graphml(G, cache_path)
    logger.info("[%s] Graph cached to %s", city.slug, cache_path)
    return G


def load_nodes_edges(
    city: CityConfig,
    G: nx.MultiDiGraph | None = None,
    force: bool = False,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Return (nodes, edges) GeoDataFrames for *city*.

    If not already cached, they are derived from *G* (which will be loaded
    automatically if not provided).

    Both GeoDataFrames share the same projected CRS as *G*.

    Parameters
    ----------
    city:
        CityConfig for the target city.
    G:
        Optional pre-loaded graph. If None, ``load_graph`` is called.
    force:
        Re-compute from graph even if cache exists.

    Returns
    -------
    Tuple[GeoDataFrame, GeoDataFrame]
        nodes, edges  — both in the graph's projected (metres) CRS.
    """
    nodes_path = _nodes_cache_path(city)
    edges_path = _edges_cache_path(city)

    if nodes_path.exists() and edges_path.exists() and not force:
        logger.info("[%s] Loading nodes/edges from cache.", city.slug)
        nodes = gpd.read_parquet(nodes_path)
        edges = gpd.read_parquet(edges_path)
        return nodes, edges

    if G is None:
        G = load_graph(city, force=force)

    logger.info("[%s] Converting graph to GeoDataFrames…", city.slug)
    nodes, edges = ox.graph_to_gdfs(G)

    # In osmnx 2.x, edges are multi-indexed by (u, v, key); reset to plain columns.
    edges = edges.reset_index()

    # Ensure length column exists in metres (OSMnx adds 'length' in metres).
    if "length" not in edges.columns:
        raise RuntimeError(
            f"[{city.slug}] Edge GeoDataFrame missing 'length' column. "
            "Check OSMnx version."
        )

    # Rename for clarity and keep only what's needed for metric computation.
    edges = edges.rename(columns={"length": "length_m"})
    edges = edges[["geometry", "length_m"]].copy()

    # Nodes: index is osmid integers; keep only geometry column.
    nodes = nodes[["geometry"]].copy()

    nodes.to_parquet(nodes_path)
    edges.to_parquet(edges_path)
    logger.info("[%s] Nodes/edges cached.", city.slug)
    return nodes, edges


def compute_intersection_nodes(
    city: CityConfig,
    G: nx.MultiDiGraph,
    nodes: gpd.GeoDataFrame,
    min_degree: int = 3,
) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame of intersection nodes (degree >= *min_degree*).

    Degree is computed on the *undirected* representation of the graph so
    that bidirectional road segments are not double-counted.

    Parameters
    ----------
    city:
        For logging only.
    G:
        Projected OSMnx graph.
    nodes:
        Nodes GeoDataFrame (projected CRS).
    min_degree:
        Minimum node degree to qualify as an intersection.

    Returns
    -------
    GeoDataFrame of intersection nodes, same CRS as *nodes*.
    """
    logger.info("[%s] Computing intersection nodes (degree >= %d)…", city.slug, min_degree)
    G_undirected = G.to_undirected()
    degree_series = pd.Series(dict(G_undirected.degree()), name="degree")
    degree_series.index.name = nodes.index.name  # align index name

    nodes_with_degree = nodes.join(degree_series, how="left")
    intersections = nodes_with_degree[nodes_with_degree["degree"] >= min_degree].copy()
    logger.info("[%s] Found %d intersection nodes.", city.slug, len(intersections))
    return intersections


def load_signals(city: CityConfig, force: bool = False) -> gpd.GeoDataFrame:
    """
    Return a GeoDataFrame of traffic signals, stop signs, and crossing signals.

    Data is fetched from OSM via OSMnx ``features_from_place`` and cached as
    Parquet. Returns an empty GeoDataFrame if no features are found.

    The returned GeoDataFrame is in EPSG:4326 (WGS 84); callers must reproject
    to match the road graph CRS before spatial operations.

    Parameters
    ----------
    city:
        CityConfig for the target city.
    force:
        Re-download even if cache exists.

    Returns
    -------
    GeoDataFrame with point geometry column in EPSG:4326.
    """
    cache_path = _signals_cache_path(city)

    if cache_path.exists() and not force:
        logger.info("[%s] Loading signals from cache.", city.slug)
        gdf = gpd.read_parquet(cache_path)
        return gdf

    logger.info("[%s] Downloading signal/stop features from OSM…", city.slug)
    frames = []

    # Query each tag group separately — OSMnx features_from_place takes a
    # dict of {tag_key: tag_value_or_list} and returns all matched features.
    try:
        gdf_all = ox.features_from_place(city.osm_query, tags=SIGNAL_TAGS)
        if len(gdf_all) > 0:
            frames.append(gdf_all)
    except Exception as exc:
        logger.warning("[%s] Signal feature download failed: %s", city.slug, exc)

    if not frames:
        logger.warning("[%s] No signal features found; using empty GeoDataFrame.", city.slug)
        empty = gpd.GeoDataFrame(geometry=gpd.GeoSeries([], dtype="geometry"), crs="EPSG:4326")
        empty.to_parquet(cache_path)
        return empty

    gdf = pd.concat(frames, ignore_index=True)

    # Normalise: ensure every row has a Point geometry.
    # OSM may return ways/polygons for traffic signals; use representative point.
    def _to_point(geom):
        if geom is None:
            return None
        if geom.geom_type == "Point":
            return geom
        return geom.representative_point()

    gdf["geometry"] = gdf["geometry"].apply(_to_point)
    gdf = gdf[gdf["geometry"].notna()].copy()
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")

    # Keep only essential columns
    keep_cols = ["geometry"]
    for col in ["highway", "crossing", "osmid"]:
        if col in gdf.columns:
            keep_cols.append(col)
    gdf = gdf[keep_cols].copy()

    gdf.to_parquet(cache_path)
    logger.info("[%s] Cached %d signal features.", city.slug, len(gdf))
    return gdf
