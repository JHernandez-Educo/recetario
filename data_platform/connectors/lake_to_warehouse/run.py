"""Promote lake tables into the Postgres warehouse.

Usage:
    uv run python -m data_platform.connectors.lake_to_warehouse.run                    # all datasets
    uv run python -m data_platform.connectors.lake_to_warehouse.run --dataset raw_vtex
"""

import argparse
import logging

import dlt

from data_platform.connectors.lake_to_warehouse.source import PROMOTED_TABLES, lake_tables


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(PROMOTED_TABLES), help="one lake dataset (default: all)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    for dataset_name in [args.dataset] if args.dataset else sorted(PROMOTED_TABLES):
        pipeline = dlt.pipeline(
            pipeline_name=f"promote_{dataset_name}",
            destination="postgres",
            dataset_name=dataset_name,
            progress="log",
        )
        print(pipeline.run(lake_tables(dataset_name)))
        counts = pipeline.last_trace.last_normalize_info.row_counts if pipeline.last_trace else {}
        print(dataset_name, "row counts:", {k: v for k, v in counts.items() if not k.startswith("_dlt")})


if __name__ == "__main__":
    main()
