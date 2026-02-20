# OSM Urbanicity Pipeline

Builds **H3 res=8 urbanicity layers** for five US cities from OpenStreetMap data.

Each H3 hexagon receives:

| Column | Description |
|--------|-------------|
| `intersection_density_per_km2` | Degree-≥3 OSM nodes per km² |
| `road_density_km_per_km2` | Drivable road km per km² (apportioned by geometric intersection) |
| `signal_density_per_km2` | Traffic signals + stop signs per km² |
| `urbanicity_score` | Weighted composite of robust z-scores |
| `urbanicity_band_3_2_1` | 3 = Very Urban · 2 = Urban · 1 = Suburban |

---

## Cities

| Slug | OSM Query |
|------|-----------|
| `seattle` | Seattle, Washington, USA |
| `los_angeles` | Los Angeles, California, USA |
| `austin` | Austin, Texas, USA |
| `chicago` | Chicago, Illinois, USA |
| `boston` | Boston, Massachusetts, USA |

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
# or editable install
pip install -e .
```

Python ≥ 3.9 required. **Note:** `h3>=3.7,<4` is required; h3 4.x has a
breaking API change and is not supported.

### 2. Run the pipeline

```bash
# All five cities
python -m urbanicity.build --cities all --h3_res 8 --buffer_m 300

# Single city
python -m urbanicity.build --cities seattle

# Multiple cities, no GeoJSON output
python -m urbanicity.build --cities chicago,boston --no_geojson

# Force re-download (ignore cache)
python -m urbanicity.build --cities austin --force

# Verbose debug output
python -m urbanicity.build --cities seattle --log_level DEBUG
```

### 3. Outputs

```
outputs/
├── seattle/
│   ├── h3_urbanicity_res8.parquet
│   ├── h3_urbanicity_res8.geojson
│   └── summary.json
├── los_angeles/
│   └── …
…
```

---

## Formulas

### Intersection Density

```
IntersectionDensity(H) = CountNodes(degree ≥ 3, within H) / Area_km2(H)
```

### Road Network Density

```
RoadNetworkDensity(H) = Σ clipped_length_m(edge ∩ H) / 1000 / Area_km2(H)
```

Edge lengths are **apportioned** by geometric intersection — each hex receives
only the portion of the road segment that lies within it.

### Signal Density

```
SignalDensity(H) = Count(highway=traffic_signals OR highway=stop
                         OR crossing=traffic_signals, within H) / Area_km2(H)
```

### Robust Z-score (per city)

```
Z(x) = (x − median(x)) / (MAD(x) + 1e-9)   clamped to [−10, +10]
```

MAD = median absolute deviation. When MAD ≈ 0 (e.g., >50% of hexes have
zero signal density so the median is 0), the pipeline falls back to the
standard deviation as the scale parameter. The final z-score is clamped to
±10 so that a single extreme outlier cannot dominate the composite score.

### Urbanicity Score

```
UrbanicityScore(H) = 0.50 · Z_int(H) + 0.30 · Z_road(H) + 0.20 · Z_sig(H)
```

If signal data is unavailable (all zeros), weights are redistributed:

```
UrbanicityScore(H) = 0.625 · Z_int(H) + 0.375 · Z_road(H)
```

### Band Discretization

Thresholds are computed within each city:

```
T_high = quantile(UrbanicityScore, 0.70)
T_low  = quantile(UrbanicityScore, 0.30)

band = 3  if score ≥ T_high   (Very Urban)
band = 1  if score ≤ T_low    (Suburban)
band = 2  otherwise           (Urban)
```

---

## Caching

OSM data is cached under `cache/`:

| File | Content |
|------|---------|
| `{city}_graph.graphml` | Projected road graph (OSMnx GraphML) |
| `{city}_nodes.parquet` | Node GeoDataFrame |
| `{city}_edges.parquet` | Edge GeoDataFrame with `length_m` |
| `{city}_signals.parquet` | Signal/stop point features |

Re-runs skip downloads automatically. Use `--force` to refresh.

---

## Parameters

| Argument | Default | Description |
|----------|---------|-------------|
| `--cities` | `all` | City slugs (comma-separated) or `all` |
| `--h3_res` | `8` | H3 resolution |
| `--buffer_m` | `300` | Boundary buffer in metres before polyfill |
| `--force` | `False` | Re-download OSM data |
| `--no_geojson` | `False` | Skip GeoJSON output |
| `--log_level` | `INFO` | Logging verbosity |

---

## Known Limitations

1. **OSM signal tagging variability**: Traffic signal and stop-sign coverage
   in OSM varies by city and contributor activity. Dense downtown cores tend
   to be well-tagged; suburban edges may be under-represented.

2. **Admin boundary vs. node convex hull**: For cities where OSMnx cannot
   geocode a clean administrative polygon, the pipeline falls back to the
   convex hull of road network nodes. This may include more area than intended
   or miss some fringe neighbourhoods.

3. **Drivable network only**: The pipeline uses `network_type="drive"`. Paths,
   pedestrian streets, and private roads are excluded. This affects
   intersection density in mixed-use or pedestrianised areas.

4. **Cross-city comparability**: Urbanicity scores are normalised within each
   city (robust z-scores). Band thresholds (0.30/0.70 quantiles) are also
   city-relative. Band 3 in Seattle is *not* directly comparable to Band 3 in
   Los Angeles; only within-city ordering is meaningful.

5. **Run time**: Los Angeles is the largest city in the dataset. Road density
   computation uses geometric overlay which may take several minutes on a
   standard laptop. The spatial-index batching in `metrics.py` limits memory
   pressure but does not eliminate the underlying computational cost.

---

## Downstream Use

The primary purpose of these layers is to provide a per-H3-hex
`urbanicity_band` that can be joined to routing/stop data:

```python
import pandas as pd

stops = pd.read_csv("stops.csv")  # must have lat/lon columns
hexes = pd.read_parquet("outputs/seattle/h3_urbanicity_res8.parquet")

import h3
stops["h3_index"] = stops.apply(
    lambda r: h3.geo_to_h3(r.lat, r.lon, 8), axis=1
)
stops = stops.merge(
    hexes[["h3_index", "urbanicity_score", "urbanicity_band_3_2_1"]],
    on="h3_index",
    how="left",
)
```

Route complexity can then be computed as any aggregation
(mean, max, distribution) of the stop-level band values.

---

## Data Sources

- **Road network**: OpenStreetMap via [OSMnx](https://osmnx.readthedocs.io/)
  (Overpass API + local GraphML caching).
- **Signal/stop features**: OpenStreetMap via OSMnx `features_from_place`.
- **H3 indexing**: [Uber H3](https://h3geo.org/) Python bindings.
- **Coordinate reference systems**: EPSG:4326 (WGS-84) for storage;
  UTM projected CRS (auto-detected per city) for metric computations.
