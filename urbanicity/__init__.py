"""
Urbanicity: OSM-derived H3 urbanicity layers for US cities.

Pipeline computes intersection density, road network density, and signal
density from OpenStreetMap data, then aggregates them into H3 hexagons
(resolution 8) with a composite urbanicity score and band (3/2/1).
"""

__version__ = "0.1.0"
