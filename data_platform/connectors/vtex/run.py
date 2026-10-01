"""Run the VTEX price pipeline for one store.

Usage:
    uv run python -m data_platform.connectors.vtex.run --store d1
    uv run python -m data_platform.connectors.vtex.run --store d1 --max-categories 2   # smoke test

Stores are defined in `.dlt/config.toml` under [sources.vtex.stores.*].
Loads into the DuckLake lake (catalog in Postgres, files in Azure), dataset `raw_vtex`.
"""

import argparse
import logging

import dlt

from data_platform.connectors.vtex.source import vtex_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True, help="store key from config.toml, e.g. d1")
    parser.add_argument("--max-categories", type=int, default=None, help="dev knob: cap leaf categories crawled")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    pipeline = dlt.pipeline(
        pipeline_name="vtex_prices",
        destination="ducklake",
        dataset_name="raw_vtex",
        progress="log",
    )
    load_info = pipeline.run(vtex_catalog(store=args.store, max_categories=args.max_categories))
    print(load_info)

    # Zero-row alerting hook (PRD R1): a run that loads nothing is a failure signal.
    row_counts = pipeline.last_trace.last_normalize_info.row_counts if pipeline.last_trace else {}
    print("row counts:", {k: v for k, v in row_counts.items() if not k.startswith("_dlt")})


if __name__ == "__main__":
    main()
