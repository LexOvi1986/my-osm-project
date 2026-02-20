"""
H3 hex grid generation.

Generates a set of H3 cells (at a given resolution) that cover a city
boundary polygon. Each cell is returned as a polygon in both WGS-84 and
the city's projected CRS for area / length calculations.
"""

from __future__ import annotations

import logging
from typing import List, Tuple

import geopandas as gpd
import h3
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
from pyproj import CRS
from shapely.geometry import MultiPolygon, Point, Polygon, mapping, shape
from shapely.ops import transform, unary_union

from urbanicity.config import CityConfig, DEFAULT_BUFFER_M, DEFAULT_H3_RES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# City boundary
# ---------------------------------------------------------------------------

def get_city_boundary(city: CityConfig, nodes_gdf: gpd.GeoDataFrame) -> Polygon:
    """
    Return a WGS-84 polygon representing the city boundary.

    Strategy (in order of preference):
    1. OSMnx geocoded administrative boundary.
    2. Convex hull of road-network nodes (fallback).

    Parameters
    ----------
    city:
        CityConfig for the city.
    nodes_gdf:
        Projected nodes GeoDataFrame (used for convex-hull fallback).

    Returns
    -------
    Shapely Polygon in EPSG:4326.
    """
    try:
        logger.info("[%s] Geocoding administrative boundary…", city.slug)
        boundary_gdf = ox.geocode_to_gdf(city.osm_query)
        boundary = boundary_gdf.geometry.iloc[0]
        # Ensure WGS-84
        if boundary_gdf.crs and boundary_gdf.crs.to_epsg() != 4326:
            boundary_gdf = boundary_gdf.to_crs("EPSG:4326")
            boundary = boundary_gdf.geometry.iloc[0]
        logger.info("[%s] Administrative boundary loaded.", city.slug)
        return boundary
    except Exception as exc:
        logger.warning(
            "[%s] Admin boundary geocode failed (%s); falling back to convex hull.",
            city.slug,
            exc,
        )

    # Fallback: convex hull of nodes reprojected to WGS-84
    nodes_wgs84 = nodes_gdf.to_crs("EPSG:4326")
    hull = nodes_wgs84.geometry.unary_union.convex_hull
    logger.info("[%s] Using convex hull of nodes as boundary.", city.slug)
    return hull


# ---------------------------------------------------------------------------
# H3 polyfill
# ---------------------------------------------------------------------------

def polyfill_boundary(
    boundary: Polygon | MultiPolygon,
    h3_res: int = DEFAULT_H3_RES,
    buffer_m: float = DEFAULT_BUFFER_M,
) -> List[str]:
    """
    Return a list of H3 cell indices covering *boundary*.

    A small metric buffer is added (by projecting to UTM, buffering, then
    projecting back) so that edge hexes that partially overlap the boundary
    are included.

    Parameters
    ----------
    boundary:
        Shapely polygon in EPSG:4326.
    h3_res:
        H3 resolution (default 8).
    buffer_m:
        Buffer in metres applied before polyfilling.

    Returns
    -------
    List of H3 index strings.
    """
    # Buffer: project → buffer → project back
    if buffer_m > 0:
        # Estimate UTM zone from centroid
        centroid = boundary.centroid
        utm_crs = _estimate_utm_crs(centroid.y, centroid.x)
        from pyproj import Transformer
        to_utm = Transformer.from_crs("EPSG:4326", utm_crs, always_xy=True)
        to_wgs = Transformer.from_crs(utm_crs, "EPSG:4326", always_xy=True)
        buffered_utm = transform(to_utm.transform, boundary).buffer(buffer_m)
        boundary = transform(to_wgs.transform, buffered_utm)

    cells: set[str] = set()

    # h3 library expects GeoJSON-like dict
    if isinstance(boundary, MultiPolygon):
        polygons = list(boundary.geoms)
    else:
        polygons = [boundary]

    for poly in polygons:
        geojson = mapping(poly)
        filled = h3.polyfill_geojson(geojson, h3_res)
        cells.update(filled)

    logger.debug("Polyfilled %d H3 cells at resolution %d.", len(cells), h3_res)
    return list(cells)


# ---------------------------------------------------------------------------
# Hex GeoDataFrame construction
# ---------------------------------------------------------------------------

def build_hex_geodataframe(
    h3_indices: List[str],
    h3_res: int,
    city: CityConfig,
    projected_crs: CRS | str,
) -> gpd.GeoDataFrame:
    """
    Build a GeoDataFrame with one row per H3 hex cell.

    Each row contains:
    - ``h3_index``: H3 cell identifier
    - ``hex_centroid_lat``, ``hex_centroid_lon``: WGS-84 centroid
    - ``geometry``: projected polygon (metres CRS) for metric computations
    - ``geometry_wgs84``: WGS-84 polygon (for GeoJSON output)
    - ``hex_area_km2``: area in km²

    Parameters
    ----------
    h3_indices:
        List of H3 cell index strings.
    h3_res:
        H3 resolution (stored as metadata column).
    city:
        CityConfig (used to populate ``city`` column).
    projected_crs:
        Metres-based CRS to project hex polygons into (should match the road
        graph CRS so that spatial joins are consistent).

    Returns
    -------
    GeoDataFrame with geometry in *projected_crs*.
    """
    logger.info(
        "[%s] Building hex GeoDataFrame for %d cells…", city.slug, len(h3_indices)
    )

    records = []
    for idx in h3_indices:
        # h3.h3_to_geo returns (lat, lng)
        lat, lon = h3.h3_to_geo(idx)
        # h3.h3_to_geo_boundary returns list of (lat, lng) tuples
        boundary_coords = h3.h3_to_geo_boundary(idx)  # [(lat, lng), ...]
        # Shapely Polygon expects (x=lon, y=lat)
        poly_wgs84 = Polygon([(lng, lat_) for lat_, lng in boundary_coords])
        records.append(
            {
                "h3_index": idx,
                "h3_res": h3_res,
                "city": city.name,
                "hex_centroid_lat": lat,
                "hex_centroid_lon": lon,
                "geometry": poly_wgs84,
            }
        )

    gdf_wgs84 = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

    # Store WGS-84 geometry before reprojecting
    gdf_wgs84["geometry_wgs84"] = gdf_wgs84["geometry"]

    # Reproject to metres CRS
    gdf_proj = gdf_wgs84.to_crs(projected_crs)

    # Compute area in km²
    gdf_proj["hex_area_km2"] = gdf_proj["geometry"].area / 1_000_000

    logger.info(
        "[%s] Hex areas: min=%.4f km², mean=%.4f km², max=%.4f km²",
        city.slug,
        gdf_proj["hex_area_km2"].min(),
        gdf_proj["hex_area_km2"].mean(),
        gdf_proj["hex_area_km2"].max(),
    )

    return gdf_proj


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _estimate_utm_crs(lat: float, lon: float) -> str:
    """Return an EPSG code string for the UTM zone containing (lat, lon)."""
    zone = int((lon + 180) / 6) + 1
    if lat >= 0:
        return f"EPSG:{32600 + zone}"
    else:
        return f"EPSG:{32700 + zone}"


def get_graph_crs(G: nx.MultiDiGraph) -> str:
    """Extract the CRS string from a projected OSMnx graph."""
    crs_val = G.graph.get("crs", "EPSG:4326")
    return str(crs_val)
