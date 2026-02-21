# OSM Urbanicity Pipeline

Builds **H3 res=8 urbanicity layers** for five US cities from OpenStreetMap data.
All outputs are **derived from OSM** (`field_semantics = "DERIVED_FROM_OSM"`).

Each H3 hexagon receives:

| Column | Type | Description |
|--------|------|-------------|
| `city` | str | City display name |
| `h3_res` | int | H3 resolution (8) |
| `h3_index` | str | H3 cell identifier |
| `hex_centroid_lat` | float | WGS-84 centroid latitude |
| `hex_centroid_lon` | float | WGS-84 centroid longitude |
| `hex_area_km2` | float | Hex area in km² |
| `intersection_density_per_km2` | float | Degree-≥3 nodes / km² |
| `road_density_km_per_km2` | float | Drivable road km / km² (apportioned) |
| `signal_density_per_km2` | float | Traffic signals + stop signs / km² |
| `z_intersection_density` | float | Robust z-score of intersection density |
| `z_road_density` | float | Robust z-score of road density |
| `z_signal_density` | float\|NaN | Robust z-score of signal density (NaN if dropped) |
| `urbanicity_score_continuous` | float | Weighted composite score |
| `urbanicity_band_3_2_1` | int | 3=Very Urban · 2=Urban · 1=Suburban |
| `t_low_q30` | float | Band 1/2 threshold (q30 of score within city) |
| `t_high_q70` | float | Band 2/3 threshold (q70 of score within city) |
| `field_semantics` | str | `"DERIVED_FROM_OSM"` |

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

Python ≥ 3.9 required.
**Note:** `h3>=3.7,<4` is required; h3 4.x has a breaking API change.

### 2. Run the pipeline

```bash
# All five cities with defaults
python -m urbanicity.build --cities all --h3_res 8

# Spec-compliant invocation (hyphens in city names are accepted)
python -m urbanicity.build \
    --cities seattle,los-angeles,austin,chicago,boston \
    --h3_res 8 --signals auto --q_low 0.30 --q_high 0.70

# Single city, re-download data
python -m urbanicity.build --cities boston --refresh

# Custom weights and quantile thresholds
python -m urbanicity.build --cities chicago \
    --weights 0.6,0.3,0.1 --q_low 0.25 --q_high 0.75

# Disable signal density entirely
python -m urbanicity.build --cities austin --signals off

# Skip GeoJSON (faster, saves disk)
python -m urbanicity.build --cities all --no_geojson
```

### 3. Outputs

```
outputs/
├── seattle/
│   ├── h3_urbanicity_res8.parquet   ← main output (17 columns)
│   ├── h3_urbanicity_res8.geojson   ← WGS-84 hex polygons
│   ├── thresholds.json              ← per-city band thresholds + weights
│   └── summary.json                 ← band distribution + top/bottom 10
├── los_angeles/ …
├── austin/      …
├── chicago/     …
└── boston/      …
```

---

## Formulas

### Intersection Density (Observed count → Derived density)

```
IntersectionDensity(H) = CountNodes(degree ≥ 3, within H) / Area_km2(H)
```

### Road Network Density (Derived, apportioned)

```
RoadNetworkDensity(H) = Σ clipped_length_m(edge ∩ H) / 1000 / Area_km2(H)
```

Edge lengths are **apportioned by geometric intersection** — each hex receives
only the fraction of each road segment that falls inside it.

### Signal Density (Derived from OSM point features)

OSM tags fetched: `highway=traffic_signals`, `highway=stop`,
`crossing=traffic_signals`.

```
SignalDensity(H) = CountSignals(within H) / Area_km2(H)
```

### Robust Z-score (per city, per metric)

```
Z(x) = (x − median(x)) / (MAD(x) + 1e-9)
```

* MAD = median absolute deviation.
* **Fallback when MAD ≈ 0**: use population standard deviation.
  If std is also ≈ 0 all values are identical and z-scores are 0.
* **Final z-scores are clamped to [−10, +10]** to prevent a single extreme
  outlier from dominating the composite.

All normalization is **per-city** — scores reflect relative urban intensity
within a city, not across cities.

### Continuous Urbanicity Score

```
UrbanicityScore(H) = 0.50·Z_int(H) + 0.30·Z_road(H) + 0.20·Z_sig(H)
```

If signal density is dropped (see sparsity rule below), weights are
renormalized to sum to 1:

```
UrbanicityScore(H) = 0.625·Z_int(H) + 0.375·Z_road(H)
```

### Signal Sparsity Rule

If **fewer than 5% of hexes** in a city contain ≥1 signal/stop feature,
signal density is considered too sparse and is **dropped** from the score.
When dropped:
* `z_signal_density` is set to `NaN`.
* Weights for intersection and road density are renormalized.
* `signals_used = false` is recorded in `thresholds.json`.

This rule applies with `--signals auto` (default). Use `--signals on` to
force inclusion or `--signals off` to always exclude.

### Band Discretization (per city)

Thresholds are computed on `urbanicity_score_continuous` within each city:

```
T_high = quantile(score, 0.70)    ← default, configurable via --q_high
T_low  = quantile(score, 0.30)    ← default, configurable via --q_low

band = 3  if score ≥ T_high   (Very Urban)
band = 1  if score ≤ T_low    (Suburban)
band = 2  otherwise           (Urban)
```

Thresholds are saved per city in `thresholds.json` and also repeated as
`t_low_q30` / `t_high_q70` columns in the Parquet file.

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--cities` | `all` | Comma-separated slugs or `all`. Hyphens/underscores both ok. |
| `--h3_res` | `8` | H3 resolution |
| `--buffer_m` | `300` | Boundary buffer in metres before polyfill |
| `--signals` | `auto` | Signal mode: `on` / `off` / `auto` (5% sparsity rule) |
| `--q_low` | `0.30` | Lower quantile threshold for band discretization |
| `--q_high` | `0.70` | Upper quantile threshold for band discretization |
| `--weights` | `0.5,0.3,0.2` | Composite weights (must sum to 1.0) |
| `--refresh` | off | Re-download OSM data (ignore cache) |
| `--no_geojson` | off | Skip GeoJSON output |
| `--log_level` | `INFO` | Logging verbosity |

---

## Caching

OSM data is cached under `cache/`:

| File | Content |
|------|---------|
| `{city}_graph.graphml` | Projected road graph (OSMnx GraphML) |
| `{city}_nodes.parquet` | Node GeoDataFrame (geometry only) |
| `{city}_edges.parquet` | Edge GeoDataFrame (`geometry`, `length_m`) |
| `{city}_signals.parquet` | Signal/stop point features |

Re-runs skip downloads automatically. Use `--refresh` to force a refresh.

---

## Validation

After each city build, the pipeline runs acceptance checks (spec §E1):

| Check | Criterion |
|-------|-----------|
| E1.1 | Hex set is non-empty |
| E1.2 | No negative density values |
| E1.3 | `urbanicity_band_3_2_1` ∈ {1, 2, 3} |
| E1.4 | `urbanicity_score_continuous` is finite for >99% of rows |
| E1.5 | When signals dropped: `z_signal_density` is all-NaN and w_sig=0 |
| E1.6 | ≥30% of hexes have intersection_density > 0 |
| E1.7 | ≥30% of hexes have road_density > 0 |

Failures are logged as errors; outputs are still written.

---

## Derived vs Observed

All columns in the H3 layer are **derived** from OSM data:

* Raw OSM data (graph, signals) = **Observed input** (not stored in outputs)
* All computed columns = **Derived** (`field_semantics = "DERIVED_FROM_OSM"`)

No field in the output represents a direct OSM observation unmodified.

---

## Known Limitations

1. **Signal tagging variability**: OSM signal/stop coverage varies by city
   and contributor. The 5% sparsity rule guards against cities where tagging
   is incomplete.

2. **Admin boundary vs. convex hull**: When OSMnx cannot geocode a clean
   administrative polygon the pipeline falls back to the convex hull of road
   network nodes. This may include water or park areas.

3. **Drivable network only**: `network_type="drive"` excludes pedestrian
   paths, cycleways, and private roads.

4. **Within-city scores only**: Urbanicity scores use per-city robust
   z-normalization. Band 3 in Seattle is **not** directly comparable to
   Band 3 in Los Angeles. Only within-city ordering is meaningful.

5. **Boston band distribution**: Boston has a large fraction of water-edge
   hexes with all-zero metrics. These cluster at the minimum score and push
   more hexes into band 1 than the nominal 30%.

---

## Downstream Use

```python
import pandas as pd, h3

stops = pd.read_csv("stops.csv")          # must have lat/lon columns
hexes = pd.read_parquet(
    "outputs/seattle/h3_urbanicity_res8.parquet"
)

stops["h3_index"] = stops.apply(
    lambda r: h3.geo_to_h3(r.lat, r.lon, 8), axis=1
)
stops = stops.merge(
    hexes[["h3_index", "urbanicity_score_continuous", "urbanicity_band_3_2_1"]],
    on="h3_index",
    how="left",
)
```

Route complexity = any aggregation (mean, max, distribution) of stop-level
`urbanicity_band_3_2_1` values.

---

## Data Sources

- **Road network**: OpenStreetMap via [OSMnx](https://osmnx.readthedocs.io/)
  (Overpass API + GraphML caching).
- **Signal/stop features**: OpenStreetMap via `ox.features_from_place`.
- **H3 indexing**: [Uber H3](https://h3geo.org/) Python bindings (3.x API).
- **CRS**: EPSG:4326 (WGS-84) for storage; auto-detected UTM zone for all
  metric computations (areas, lengths).
