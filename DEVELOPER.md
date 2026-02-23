# Developer Guide — OSM Urbanicity Pipeline

This guide is aimed at developers joining the routing project who need to understand,
run, or integrate the urbanicity pipeline.

For full formula and schema details see [README.md](README.md).

---

## Table of Contents

1. [What This Produces](#what-this-produces)
2. [Environment Setup](#environment-setup)
3. [Project Structure](#project-structure)
4. [Running the Pipeline](#running-the-pipeline)
5. [Understanding the Outputs](#understanding-the-outputs)
6. [Integrating with the Routing Project](#integrating-with-the-routing-project)
7. [Adding a New City](#adding-a-new-city)
8. [Pipeline Internals](#pipeline-internals)
9. [Troubleshooting](#troubleshooting)

---

## What This Produces

For each of 5 US cities (Seattle, Los Angeles, Austin, Chicago, Boston) the pipeline
outputs a Parquet file where every row is an **H3 resolution-8 hex cell** (~0.74 km²)
annotated with:

| Column | Routing use |
|--------|-------------|
| `h3_index` | Join key — map any lat/lon to this using `h3.geo_to_h3(lat, lon, 8)` |
| `urbanicity_score_continuous` | Continuous score; use for weighted cost adjustments |
| `urbanicity_band_3_2_1` | Discrete tier: **3=Very Urban · 2=Urban · 1=Suburban** |
| `intersection_density_per_km2` | Proxy for turn complexity / signal stops |
| `road_density_km_per_km2` | Network coverage within the hex |
| `signal_density_per_km2` | Traffic control density |

The band is the primary field for routing segmentation. The continuous score is useful
when you need finer-grained differentiation within a band.

---

## Environment Setup

### Requirements

- Python ≥ 3.9
- ~2 GB disk for OSM cache (downloaded once, reused on re-runs)
- Internet access on first run (OSM Overpass API)

### Install

```bash
# Clone the repo
git clone https://github.com/LexOvi1986/my-osm-project.git
cd my-osm-project

# Install dependencies (editable install recommended for development)
pip install -e .

# Or plain install from requirements
pip install -r requirements.txt
```

### Key dependencies

| Package | Purpose |
|---------|---------|
| `osmnx` | OSM road network download + graph operations |
| `h3>=3.7,<4` | H3 hexagonal indexing (**must be 3.x — h3 4.x has a breaking API**) |
| `geopandas` | Spatial joins, geometry clipping |
| `pyarrow` | Parquet read/write |
| `shapely` | Geometry operations |

---

## Project Structure

```
my-osm-project/
├── urbanicity/
│   ├── config.py      # Constants, city definitions, weights, output schema
│   ├── osm.py         # OSM download + caching (graph, nodes, edges, signals)
│   ├── h3grid.py      # H3 polyfill + hex GeoDataFrame construction
│   ├── metrics.py     # Per-hex intersection / road / signal density
│   ├── score.py       # Z-score normalization, composite score, band assignment
│   ├── validate.py    # Post-build acceptance checks (E1.1–E1.7)
│   ├── io.py          # Output writers (Parquet, GeoJSON, thresholds, summary)
│   ├── pipeline.py    # Orchestrator — ties all steps together
│   └── cli.py         # argparse CLI entrypoint
├── outputs/
│   ├── seattle/
│   │   ├── h3_urbanicity_res8.parquet   ← main output
│   │   ├── h3_urbanicity_res8.geojson   ← WGS-84 hex polygons for mapping
│   │   ├── thresholds.json              ← band thresholds + effective weights
│   │   └── summary.json                 ← band distribution + top/bottom 10 hexes
│   ├── los_angeles/
│   ├── austin/
│   ├── chicago/
│   └── boston/
├── cache/                               ← OSM data (gitignored, auto-populated)
├── README.md                            ← Formula and schema reference
├── DEVELOPER.md                         ← This file
├── requirements.txt
└── pyproject.toml
```

---

## Running the Pipeline

### First run (downloads OSM data)

```bash
# All 5 cities with defaults
python -m urbanicity.build --cities all

# Single city
python -m urbanicity.build --cities boston
```

OSM data is downloaded via the Overpass API and cached under `cache/`. Subsequent
runs skip the download automatically.

### Re-run with cached data (fast)

```bash
python -m urbanicity.build --cities all
# Each city takes ~10-30 seconds from cache
```

### Force re-download

```bash
python -m urbanicity.build --cities all --refresh
```

### Common options

```bash
# Skip GeoJSON (faster, saves ~50 MB per city)
python -m urbanicity.build --cities all --no_geojson

# Custom band thresholds (widen the Urban middle band)
python -m urbanicity.build --cities all --q_low 0.25 --q_high 0.75

# Custom score weights (intersection-heavy)
python -m urbanicity.build --cities chicago --weights 0.6,0.3,0.1

# Disable signal density (use if OSM signal tagging is incomplete)
python -m urbanicity.build --cities austin --signals off
```

---

## Understanding the Outputs

### Parquet schema (17 columns)

```python
import pandas as pd

df = pd.read_parquet("outputs/seattle/h3_urbanicity_res8.parquet")
print(df.dtypes)
# city                              object
# h3_res                             int64
# h3_index                          object   ← H3 cell ID string
# hex_centroid_lat                 float64
# hex_centroid_lon                 float64
# hex_area_km2                     float64
# intersection_density_per_km2     float64
# road_density_km_per_km2          float64
# signal_density_per_km2           float64
# z_intersection_density           float64
# z_road_density                   float64
# z_signal_density                 float64   ← NaN if signals dropped
# urbanicity_score_continuous      float64
# urbanicity_band_3_2_1              int64   ← 1 / 2 / 3
# t_low_q30                        float64   ← band 1/2 cut-off (same value all rows)
# t_high_q70                       float64   ← band 2/3 cut-off (same value all rows)
# field_semantics                   object   ← "DERIVED_FROM_OSM"
```

### thresholds.json

```json
{
  "city": "Seattle",
  "slug": "seattle",
  "h3_res": 8,
  "t_low_q30": -0.8,
  "t_high_q70": 1.1447,
  "signals_used": true,
  "weights_effective": {
    "w_intersection": 0.5,
    "w_road": 0.3,
    "w_signal": 0.2
  },
  "field_semantics": "DERIVED_FROM_OSM"
}
```

### Band interpretation

| Band | Label | Meaning for routing |
|------|-------|---------------------|
| 3 | Very Urban | Dense intersections, high signal count — expect slower speeds, complex turns |
| 2 | Urban | Moderate density — typical city driving |
| 1 | Suburban | Low density — faster, simpler network |

> **Note:** Bands are per-city. Band 3 in Seattle is not directly comparable to
> Band 3 in Los Angeles. Only within-city ordering is meaningful.

---

## Integrating with the Routing Project

### Step 1 — Load the urbanicity layer

```python
import pandas as pd

# Load one city
hexes = pd.read_parquet("outputs/seattle/h3_urbanicity_res8.parquet")

# Or load all cities at once
import glob
hexes_all = pd.concat([
    pd.read_parquet(p) for p in glob.glob("outputs/*/h3_urbanicity_res8.parquet")
], ignore_index=True)
```

### Step 2 — Assign urbanicity to stops or waypoints

```python
import h3

stops = pd.read_csv("stops.csv")   # must have 'lat' and 'lon' columns

stops["h3_index"] = stops.apply(
    lambda r: h3.geo_to_h3(r.lat, r.lon, 8), axis=1
)

stops = stops.merge(
    hexes[["h3_index", "urbanicity_score_continuous", "urbanicity_band_3_2_1"]],
    on="h3_index",
    how="left",
)
```

### Step 3 — Use bands in routing logic

**Option A — Discrete tier lookup (simple)**

```python
SPEED_FACTOR = {3: 0.6, 2: 0.8, 1: 1.0}   # Very Urban is slowest

stops["adjusted_dwell_time"] = stops["urbanicity_band_3_2_1"].map(
    lambda b: base_dwell_time / SPEED_FACTOR.get(b, 1.0)
)
```

**Option B — Continuous score (smoother)**

```python
import numpy as np

# Normalise score to [0, 1] within city before applying
score = stops["urbanicity_score_continuous"]
stops["urban_weight"] = (score - score.min()) / (score.max() - score.min() + 1e-9)

# Example: penalise urban routes in a cost function
stops["cost_multiplier"] = 1.0 + 0.5 * stops["urban_weight"]
```

**Option C — Route-level urbanicity summary**

```python
# Aggregate stop-level bands to a route-level metric
route_summary = (
    stops.groupby("route_id")["urbanicity_band_3_2_1"]
    .agg(["mean", "max", lambda x: (x == 3).mean()])
    .rename(columns={"mean": "avg_band", "max": "max_band", "<lambda_0>": "pct_very_urban"})
)
```

### Missing hex handling

Stops that fall outside the city boundary will get `NaN` after the merge.
Always fill or filter before downstream use:

```python
# Option 1: fill unmatched stops with suburban (band 1)
stops["urbanicity_band_3_2_1"] = stops["urbanicity_band_3_2_1"].fillna(1).astype(int)

# Option 2: drop unmatched stops
stops = stops.dropna(subset=["urbanicity_band_3_2_1"])
```

---

## Adding a New City

1. Open `urbanicity/config.py` and add an entry to `CITIES`:

```python
CITIES: Dict[str, CityConfig] = {
    ...
    "denver": CityConfig(
        name="Denver",
        slug="denver",
        osm_query="Denver, Colorado, USA",
    ),
}
```

2. Update `ALL_CITY_SLUGS` (it is derived automatically from `CITIES.keys()`).

3. Run the pipeline:

```bash
python -m urbanicity.build --cities denver
```

OSM data will be downloaded and cached on first run.

---

## Pipeline Internals

Each city goes through 10 steps in `pipeline.py`:

```
Step 1  Load road graph          osmnx GraphML → networkx graph
Step 2  Load nodes/edges         GeoDataFrames in projected UTM CRS
Step 3  Intersection nodes       degree ≥ 3 filter
Step 4  Signal features          traffic_signals + stop + crossing tags
Step 5  H3 polyfill              city boundary → list of H3 cell IDs
Step 6  Per-hex metrics          spatial join / overlay for each metric
Step 7  Score + bands            robust z-score → composite → quantile bands
Step 8  field_semantics          annotate "DERIVED_FROM_OSM"
Step 9  Validation               E1.1–E1.7 acceptance checks
Step 10 Write outputs            Parquet + GeoJSON + thresholds.json + summary.json
```

To run the pipeline programmatically (without CLI):

```python
from urbanicity.config import CITIES
from urbanicity.pipeline import run_city

gdf = run_city(
    city=CITIES["chicago"],
    h3_res=8,
    signal_mode="auto",
    q_low=0.30,
    q_high=0.70,
    emit_geojson=False,
)
print(gdf[["h3_index", "urbanicity_score_continuous", "urbanicity_band_3_2_1"]].head())
```

---

## Troubleshooting

### `h3` install fails with CMake error
Pin to h3 3.x: `pip install "h3>=3.7,<4"`. h3 4.x requires CMake and has a
breaking API change.

### `KeyError` on edge columns
Caused by osmnx 2.x returning a MultiIndex on edges. Already handled in `osm.py`
via `reset_index()`. If you upgrade osmnx, re-check this.

### `Object of type bool is not JSON serializable`
Fixed in `io.py` via `_SafeEncoder`. If you add new `bool` or numpy types to
`gdf.attrs`, ensure they flow through `_SafeEncoder` or are cast to native Python
types before `json.dump`.

### Overpass API timeout / rate limit
On first run for large cities (LA, Austin) the Overpass download can take several
minutes. If it times out, re-run — osmnx retries automatically. Use `--refresh`
only when you need fresh OSM data, not to retry a failed download.

### Validation failures logged but outputs still written
Validation (step 9) is non-fatal by design. Check the logged `ERROR` lines for
which E1 check failed. Common causes:
- E1.6/E1.7: city boundary includes large water/park areas with zero road coverage
- E1.5: `--signals on` forced with a very sparse city
