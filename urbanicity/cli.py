"""
CLI entrypoint for the urbanicity pipeline.

Usage
-----
    python -m urbanicity.build --cities all --h3_res 8 --buffer_m 300

or via the installed script:

    urbanicity --cities seattle chicago --h3_res 8
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List

from urbanicity.config import ALL_CITY_SLUGS, CITIES, DEFAULT_BUFFER_M, DEFAULT_H3_RES
from urbanicity.pipeline import run_city


def _parse_cities(value: str) -> List[str]:
    """Parse the --cities argument into a list of slugs."""
    if value.lower() == "all":
        return ALL_CITY_SLUGS
    slugs = [s.strip().lower() for s in value.split(",")]
    unknown = [s for s in slugs if s not in CITIES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"Unknown city slug(s): {unknown}. "
            f"Valid choices: {ALL_CITY_SLUGS}"
        )
    return slugs


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
            "  python -m urbanicity.build --cities all",
            "  python -m urbanicity.build --cities seattle,chicago --h3_res 8",
            "  python -m urbanicity.build --cities austin --force",
        ]),
    )

    parser.add_argument(
        "--cities",
        default="all",
        metavar="CITY[,CITY,...]|all",
        help=(
            "Comma-separated city slugs or 'all'. "
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
            "Buffer in metres applied to city boundary before H3 polyfill "
            f"(default: {DEFAULT_BUFFER_M})"
        ),
    )
    parser.add_argument(
        "--no_geojson",
        action="store_true",
        help="Skip GeoJSON output (saves time/disk for large cities).",
    )
    parser.add_argument(
        "--force",
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


def main(argv: List[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Configure logging
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

    logger = logging.getLogger(__name__)
    logger.info(
        "Starting urbanicity pipeline for cities: %s | H3 res=%d | buffer=%.0f m",
        city_slugs,
        args.h3_res,
        args.buffer_m,
    )

    failed = []
    for slug in city_slugs:
        city = CITIES[slug]
        try:
            run_city(
                city=city,
                h3_res=args.h3_res,
                buffer_m=args.buffer_m,
                write_geojson=not args.no_geojson,
                force=args.force,
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
