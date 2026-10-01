"""Dagster definitions: dlt connectors + dbt transforms in one asset graph.

Native integrations only: `dagster-dlt` wraps the connectors, `dagster-dbt`
wraps the transform project. Asset keys are table-grain and match dbt's
source declarations exactly ([raw_vtex, products] etc.), so Dagster draws
the full lineage raw -> clean -> marts automatically and orders ingestion
before transformation inside the weekly job.

Execution model is deliberately lightweight (no 24/7 daemon required):
    uv run dagster dev -m data_platform.orchestration.definitions   # local UI + schedules
    uv run dagster job execute -m data_platform.orchestration.definitions -j weekly_pipeline
The second form is what a cron entry or a wake-run-exit Docker container calls.

The weekly job uses the in-process executor ON PURPOSE: stores are crawled
sequentially (polite ~1 req/s), and the dlt pipelines share their state with
the CLI runners in data_platform/connectors/ (both restore it from the lake).
"""

import hashlib
import os
import urllib.request

import dlt as dltlib
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    in_process_executor,
    run_failure_sensor,
    sensor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from data_platform.connectors.lake_to_warehouse.source import PROMOTED_TABLES, lake_tables
from data_platform.connectors.recipes.source import INCOMING_DIR, archive_incoming, recipe_files
from data_platform.connectors.shopify.source import shopify_catalog
from data_platform.connectors.vtex.source import PROJECT_ROOT, vtex_catalog

TRANSFORM_DIR = PROJECT_ROOT / "data_platform" / "transform"

dbt_project = DbtProject(project_dir=TRANSFORM_DIR, profiles_dir=TRANSFORM_DIR)
dbt_project.prepare_if_dev()


class LakeTranslator(DagsterDltTranslator):
    """Lake assets are keyed [lake, dataset, table] (e.g. lake/raw_vtex/products):
    the raw landing zone in DuckLake, root of the graph."""

    def __init__(self, dataset: str):
        self.dataset = dataset

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(key=AssetKey(["lake", self.dataset, data.resource.name]), deps=[])


class WarehouseRawTranslator(DagsterDltTranslator):
    """Promoted tables are keyed [dataset, table] (e.g. raw_vtex/products) — the
    same keys dbt derives for its sources — and depend on their lake twin, which
    stitches lake -> warehouse -> dbt into one lineage graph."""

    def __init__(self, dataset: str):
        self.dataset = dataset

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(
            key=AssetKey([self.dataset, data.resource.name]),
            deps=[AssetKey(["lake", self.dataset, data.resource.name])],
        )


@dlt_assets(
    dlt_source=vtex_catalog(),  # store=None -> all configured stores, sequentially
    dlt_pipeline=dltlib.pipeline(
        pipeline_name="vtex_prices",  # same pipeline as the CLI runner -> shared incremental state
        destination="ducklake",
        dataset_name="raw_vtex",
        progress="log",
    ),
    name="vtex",
    group_name="ingestion",
    dagster_dlt_translator=LakeTranslator("raw_vtex"),
)
def vtex_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=shopify_catalog(),
    dlt_pipeline=dltlib.pipeline(
        pipeline_name="shopify_prices",
        destination="ducklake",
        dataset_name="raw_shopify",
        progress="log",
    ),
    name="shopify",
    group_name="ingestion",
    dagster_dlt_translator=LakeTranslator("raw_shopify"),
)
def shopify_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=recipe_files(),
    dlt_pipeline=dltlib.pipeline(
        pipeline_name="recipes",
        destination="ducklake",
        dataset_name="raw_recipes",
        progress="log",
    ),
    name="recipes",
    group_name="ingestion",
    dagster_dlt_translator=LakeTranslator("raw_recipes"),
)
def recipe_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
    moved = archive_incoming()  # only reached after a successful load
    context.log.info("archived %d recipe file(s) to processed/", moved)


def _promotion_assets(dataset: str):
    @dlt_assets(
        dlt_source=lake_tables(dataset),
        dlt_pipeline=dltlib.pipeline(
            pipeline_name=f"promote_{dataset}",  # same pipeline as the CLI runner
            destination="postgres",
            dataset_name=dataset,
            progress="log",
        ),
        name=f"promote_{dataset}",
        group_name="promotion",
        dagster_dlt_translator=WarehouseRawTranslator(dataset),
    )
    def _assets(context: AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    return _assets


promotion_assets = [_promotion_assets(d) for d in sorted(PROMOTED_TABLES)]


class TransformTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props) -> str:
        return "transform"

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        """Key models, snapshots and seeds by the table they build ([schema, alias],
        e.g. clean_vtex/price_events), so the UI matches the database. Sources keep
        dbt's default [source, table] key, which is what links them to the warehouse raw assets."""
        if dbt_resource_props["resource_type"] in ("model", "snapshot", "seed"):
            return AssetKey([dbt_resource_props["schema"], dbt_resource_props.get("alias") or dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=TransformTranslator(),
)
def transform_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models, snapshots, and tests; tests surface as asset checks."""
    yield from dbt.cli(["build"], context=context).stream()


weekly_pipeline = define_asset_job(
    name="weekly_pipeline",
    selection=AssetSelection.groups("ingestion", "promotion", "transform"),
    executor_def=in_process_executor,  # sequential by design
)

weekly_schedule = ScheduleDefinition(
    job=weekly_pipeline,
    cron_schedule="0 2 * * 1",  # Mondays 02:00 — night run per PRD hygiene
    execution_timezone="America/Bogota",
)

recipes_job = define_asset_job(
    name="recipes_job",
    selection=AssetSelection.keys(AssetKey(["lake", "raw_recipes", "recipes"])),
    executor_def=in_process_executor,
)


@sensor(job=recipes_job, minimum_interval_seconds=60)
def recipe_files_sensor(context: SensorEvaluationContext):
    """New JSON in data_lake/recipes/incoming/ -> run the recipe loader.
    The run_key (hash of names + mtimes) makes triggers idempotent: the same
    batch of files never launches twice."""
    files = sorted(INCOMING_DIR.glob("*.json")) if INCOMING_DIR.exists() else []
    if not files:
        return SkipReason("no incoming recipe files")
    fingerprint = hashlib.sha256(
        "|".join(f"{p.name}:{p.stat().st_mtime_ns}" for p in files).encode()
    ).hexdigest()[:16]
    return RunRequest(run_key=f"recipes-{fingerprint}")


@run_failure_sensor
def notify_on_failure(context):
    """Any failed run -> phone push via ntfy.sh (set NTFY_TOPIC env var to
    enable; without it, failures are still logged and visible in the UI)."""
    message = f"Recetario run FAILED: {context.dagster_run.job_name}\n{context.failure_event.message or ''}"[:800]
    context.log.error(message)
    topic = os.getenv("NTFY_TOPIC")
    if not topic:
        return
    request = urllib.request.Request(
        f"https://ntfy.sh/{topic}",
        data=message.encode(),
        headers={"Title": "Recetario pipeline failure", "Priority": "high"},
    )
    urllib.request.urlopen(request, timeout=10)


defs = Definitions(
    executor=in_process_executor,  # every job, including UI materializations, runs one step at a time
    assets=[vtex_assets, shopify_assets, recipe_assets, *promotion_assets, transform_models],
    resources={
        "dlt": DagsterDltResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
    jobs=[weekly_pipeline, recipes_job],
    schedules=[weekly_schedule],
    sensors=[recipe_files_sensor, notify_on_failure],
)
