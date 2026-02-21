"""
CLI entrypoint for the urbanicity pipeline.

Usage
-----
    python -m urbanicity.build --cities all --h3_res 8 --buffer_m 300
    python -m urbanicity.build --cities seattle,los-angeles,austin,chicago,boston \\
        --h3_res 8 --refresh --signals auto --q_low 0.30 --q_high 0.70

or via the installed script:

    urbanicity --cities all --h3_res 8
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional, Tuple

from urbanicity.config import (
    ALL_CITY_SLUGS,
    CITIES,
    DEFAULT_BUFFER_M,
    DEFAULT_H3_RES,
    DEFAULT_SIGNAL_MODE,
    QUANTILE_HIGH,
    QUANTILE_LOW,
)
from urbanicity.pipeline import run_city


# ---------------------------------------------------------------------------
# Argument parsing helpers
# ---------------------------------------------------------------------------

def _normalise_slug(raw: str) -> str:
    """Lower-case and replace hyphens with underscores."""
    return raw.strip().lower().replace("-", "_")


def _parse_cities(value: str) -> List[str]:
    """Parse the --cities argument into a list of canonical slugs."""
    if value.lower() == "all":
        return ALL_CITY_SLUGS
    slugs = [_normalise_slug(s) for s in value.split(",")]
    unknown = [s for s in slugs if s not in CITIES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"Unknown city slug(s): {unknown}. "
            f"Valid choices: {ALL_CITY_SLUGS}"
        )
    return slugs


def _parse_weights(value: str) -> Tuple[float, float, float]:
    """Parse 'w_int,w_road,w_sig' into a validated (float, float, float)."""
    parts = value.split(",")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "--weights requires exactly 3 comma-separated floats: "
            "w_intersection,w_road,w_signal  (e.g. 0.5,0.3,0.2)"
        )
    try:
        w = tuple(float(p.strip()) for p in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--weights parse error: {exc}") from exc

    total = sum(w)
    if abs(total - 1.0) > 0.01:
        raise argparse.ArgumentTypeError(
            f"--weights must sum to 1.0 (got {total:.4f}: {w})"
        )
    return w  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="urbanicity",
        description=(
            "Compute OSM-derived H3 urbanicity layers "
            "(intersection density, road density, signal density) "
            "for one or more US cities."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join([
            "Examples:",
            "  # run all cities with defaults",
            "  python -m urbanicity.build --cities all",
            "",
            "  # specific cities (hyphens or underscores both accepted)",
            "  python -m urbanicity.build --cities seattle,los-angeles,boston",
            "",
            "  # custom quantile thresholds and weights",
            "  python -m urbanicity.build --cities chicago \\",
            "      --q_low 0.25 --q_high 0.75 --weights 0.6,0.3,0.1",
            "",
            "  # force re-download of OSM data",
            "  python -m urbanicity.build --cities austin --refresh",
            "",
            "  # disable signal density entirely",
            "  python -m urbanicity.build --cities boston --signals off",
        ]),
    )

    parser.add_argument(
        "--cities",
        default="all",
        metavar="CITY[,CITY,...]|all",
        help=(
            "Comma-separated city slugs or 'all'. "
            "Hyphens and underscores are both accepted. "
            f"Valid slugs: {', '.join(ALL_CITY_SLUGS)}. "
            "Default: all"
        ),
    )
    parser.add_argument(
        "--h3_res",
        type=int,
        default=DEFAULT_H3_RES,
        metavar="INT",
        help=f"H3 resolution (default: {DEFAULT_H3_RES})",
    )
    parser.add_argument(
        "--buffer_m",
        type=float,
        default=DEFAULT_BUFFER_M,
        metavar="METRES",
        help=(
            "Buffer in metres applied to city boundary before H3 polyfill. "
            f"Default: {DEFAULT_BUFFER_M}"
        ),
    )
    parser.add_argument(
        "--signals",
        default=DEFAULT_SIGNAL_MODE,
        choices=["on", "off", "auto"],
        metavar="on|off|auto",
        help=(
            "Signal density inclusion mode. "
            "'auto' (default): drop if <5%% of hexes have ≥1 signal. "
            "'on': always include. "
            "'off': always drop."
        ),
    )
    parser.add_argument(
        "--q_low",
        type=float,
        default=QUANTILE_LOW,
        metavar="FLOAT",
        help=f"Lower quantile for band 1 threshold (default: {QUANTILE_LOW})",
    )
    parser.add_argument(
        "--q_high",
        type=float,
        default=QUANTILE_HIGH,
        metavar="FLOAT",
        help=f"Upper quantile for band 3 threshold (default: {QUANTILE_HIGH})",
    )
    parser.add_argument(
        "--weights",
        type=_parse_weights,
        default=None,
        metavar="W_INT,W_ROAD,W_SIG",
        help=(
            "Comma-separated score weights summing to 1.0 "
            "(default: 0.5,0.3,0.2). "
            "When signals are dropped the signal weight is redistributed."
        ),
    )
    parser.add_argument(
        "--no_geojson",
        action="store_true",
        help="Skip GeoJSON output.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download OSM data even if cache files exist.",
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("fiona").setLevel(logging.WARNING)

    try:
        city_slugs = _parse_cities(args.cities)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
        return  # unreachable; keeps type-checker happy

    logger = logging.getLogger(__name__)
    logger.info(
        "Pipeline starting | cities=%s | h3_res=%d | signals=%s | "
        "q_low=%.2f | q_high=%.2f | weights=%s | refresh=%s",
        city_slugs,
        args.h3_res,
        args.signals,
        args.q_low,
        args.q_high,
        args.weights if args.weights else "default",
        args.refresh,
    )

    failed = []
    for slug in city_slugs:
        city = CITIES[slug]
        try:
            run_city(
                city=city,
                h3_res=args.h3_res,
                buffer_m=args.buffer_m,
                signal_mode=args.signals,
                weights=args.weights,
                q_low=args.q_low,
                q_high=args.q_high,
                emit_geojson=not args.no_geojson,
                force=args.refresh,
            )
        except Exception as exc:
            logger.error("[%s] Pipeline failed: %s", slug, exc, exc_info=True)
            failed.append(slug)

    if failed:
        logger.error("Pipeline completed with failures in: %s", failed)
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully for all cities.")


if __name__ == "__main__":
    main()
