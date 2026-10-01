"""Promote raw tables from the lake (DuckLake) into the Postgres warehouse.

The lake keeps everything dlt loads; the warehouse gets only the tables dbt
models. Promoted tables are explicit config below, not discovered.
Append tables move only new rows (dlt incremental cursor on the lake's
`_dlt_load_id`); small current-state tables are replaced each run.
"""

import dlt

PROMOTED_TABLES: dict[str, dict[str, str]] = {
    "raw_vtex": {"categories": "replace", "products": "replace", "price_events": "append"},
    "raw_shopify": {"products": "replace", "products__variants": "replace", "price_events": "append"},
}

CHUNK_SIZE = 50_000


def _table_resource(lake: dlt.Dataset, table: str, disposition: str):
    if disposition == "append":

        @dlt.resource(name=table, write_disposition="append")
        def rows(cursor=dlt.sources.incremental("_dlt_load_id", initial_value="0")):
            yield from lake.table(table).incremental(cursor).iter_arrow(chunk_size=CHUNK_SIZE)

    else:

        @dlt.resource(name=table, write_disposition="replace")
        def rows():
            yield from lake.table(table).iter_arrow(chunk_size=CHUNK_SIZE)

    return rows


@dlt.source(name="lake_to_warehouse")
def lake_tables(dataset_name: str):
    """The configured tables of one lake dataset, read from DuckLake."""
    lake = dlt.dataset(destination="ducklake", dataset_name=dataset_name)
    return [_table_resource(lake, t, d) for t, d in PROMOTED_TABLES[dataset_name].items()]
