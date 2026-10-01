"""Load extracted recipe JSON files into the warehouse.

Usage:
    uv run python -m data_platform.connectors.recipes.run

Reads data_lake/recipes/incoming/*.json (contract: recipe.schema.json),
loads to dataset `raw_recipes`, then archives loaded files to processed/.
Invalid files land in rejected/ with a loud log line.
"""

import logging

import dlt

from data_platform.connectors.recipes.source import archive_incoming, recipe_files


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    pipeline = dlt.pipeline(
        pipeline_name="recipes",
        destination="ducklake",
        dataset_name="raw_recipes",
        progress="log",
    )
    load_info = pipeline.run(recipe_files())
    print(load_info)
    print(f"archived {archive_incoming()} file(s) to processed/")


if __name__ == "__main__":
    main()
