# Platform guide: how to read Recetario

This guide is for operators who are new to dlt, dbt or Dagster. It covers which
tool answers which question, how to check platform health in five minutes,
how to follow one number from source to report, and what to do when something
fails.

## The mental model: four windows, four questions

You never "use" dlt or dbt directly to watch the platform. Dagster runs them,
and each window answers a different question.

| Question | Window | What it shows |
|---|---|---|
| **Is it running? Did last night work? What happened, when?** | **Dagster UI** | Runs, schedules, sensors, asset status, test results, logs and timings |
| **What does this table or column mean, and how is it built?** | **dbt docs site** | Model and column descriptions, the SQL, tests per model, and lineage between models |
| **What's actually in the data?** | **A SQL client** (DBeaver, psql) | The rows. Query the warehouse and the lake catalog |
| **Are the machine and the storage OK? What does it cost?** | **Azure portal** | VM health and metrics, lake files in storage, and costs and budgets |

The idea in one line: **Dagster is the control room, dbt docs is the manual,
SQL is the microscope, and Azure is the building.**

- **dlt** has no UI of its own here. Its work shows up in Dagster: each load is an asset materialization, with `rows_loaded`, timing and the dlt progress lines in the step logs.
- **dbt** shows up in two places. Its models are Dagster assets and its tests are Dagster asset checks. Its documentation is the static docs site.

## The five-minute health check (Monday morning)

1. **Dagster → Overview.** Did the weekly run succeed? Look at the timeline:
   green means it succeeded, red means it failed.
2. **Dagster → Assets → View lineage.** Scan the graph for red or stale
   assets. It reads left to right:
   - `lake/…` is what dlt landed;
   - `raw_*/…` is what was promoted into the warehouse;
   - `clean_vtex/…`, `clean_shopify/…`, `snapshots_*/…` and `marts/…` are dbt. Each clean schema is one source; the marts combine them.
3. **Click a mart, such as `marts/price_events_FACT`, then open the Checks tab.**
   Every dbt test on that model is listed with its last result.
4. **Dagster → Runs → the latest run.** It shows step durations: ingestion
   per source, promotion, then dbt. For a long or failed step, open the
   step's **stdout/stderr** to see dlt's progress lines (rows, rate, memory).
5. **Optional, a SQL sanity check:**
   ```sql
   SELECT s.store_key, count(*) AS events, max(f.captured_at) AS latest
   FROM marts."price_events_FACT" f
   JOIN marts."stores_DIM" s ON s."store_id_PK" = f."store_id_FK"
   GROUP BY 1 ORDER BY 1;
   ```
   Every store should have a recent `latest` value.

## Follow the data story: one number, end to end

Take the price per gram of flour at D1:

1. **Where did it come from?** In Dagster, open `lake/raw_vtex/products`.
   Materializations show when it was loaded and how many rows came in. In
   Azure storage, the Parquet files sit under
   `ducklake/raw_vtex/products/store=d1/`.
2. **How did it reach the warehouse?** Next is `raw_vtex/products` in the
   **promotion** group. That's the copy from lake to warehouse.
3. **How was it cleaned?** Next is `clean_vtex/products`. Open it in the **dbt
   docs site** to read how each retailer's package size becomes `net_grams`,
   with the exact SQL.
4. **Where is the measure computed?** `marts/price_events_FACT` holds
   `price_per_gram_cop = price_cop / net_grams`. Its lineage tab in dbt docs
   shows every upstream model.
5. **What does it say right now?** Query it in your SQL client.

## When something fails

**How the gates work:**
- **Dagster runs steps in order:** ingest, then promote, then transform. If a
  step fails, the steps after it don't run.
- **Inside dbt,** a failing error-level test skips every model downstream of
  it. Warn-level tests report but don't block.
- **The marts keep their last good version.** Data may be stale, but it is
  never wrong.

**What to do:**
1. Open the failed run in Dagster. The red step shows the error, and the step
   logs show the detail.
2. Read the error to see which test or step failed and why. For a dbt test,
   the message names the test and the model. The same test is in that model's
   `schema.yml`.
3. Fix the cause, either the data or the code. Then use **Re-execute → From
   failure** on the run, or select the affected assets and **Materialize**.

**What to fix where:** the duplicates policy says raw keeps everything and the
clean layers deduplicate (most frequent price in a crawl, then lowest). A duplicate found downstream of clean is a defect
to fix at its source.

## Where things live

| Thing | Location |
|---|---|
| Pipeline code | `data_platform/connectors/` (dlt), `data_platform/transform/` (dbt), `data_platform/orchestration/` (Dagster) |
| Schedules and sensors | `data_platform/orchestration/definitions.py`. On/off state is stored in Dagster, so you toggle them in the UI |
| Tests | `schema.yml` files and `tests/` in `data_platform/transform/` |
| Settings | `.dlt/config.toml` (non-secret), `.env` and `.dlt/secrets.toml` (secret, never committed) |
| dbt docs site | regenerated with `dbt docs generate --static`, published to the `gh-pages` branch |

## Robustness roadmap

| Component | In place | Next |
|---|---|---|
| **dlt** | HTTP retries with exponential backoff (5 attempts), timeouts, polite pacing, state stored with the data | Zero-row and per-store row-count checks. Schema contracts to catch a source changing its fields. One asset per store, so one store failing doesn't block the others |
| **Dagster** | Ordered steps, run history in Postgres, failure sensor, one-step-at-a-time execution | `RetryPolicy` with exponential backoff on ingestion assets. Run-level retries. Freshness checks ("this table should update weekly"). Slack and phone alerts. Per-store concurrency pools for parallel crawls |
| **dbt** | 42 blocking tests, 2 warnings, 2 unit tests for the dedupe rule, incremental models, SCD2 snapshots | Source freshness checks. Unit tests for the package-size parsing. Model contracts that enforce column types |
| **Postgres** | Persistent volume, idempotent bootstrap, least-privilege roles | Nightly backups to the lake container, with a tested restore. Container memory limits. Disk monitoring |
| **DuckLake** | Relative file paths, soft delete on storage, partitioning | Scheduled maintenance: merge small files and expire old snapshots |
| **VM and Docker** | Restart policies, private UI and database ports | Log rotation limits, health checks on every service, automatic OS security updates, disk snapshots |
| **Delivery** | CI validates dbt and Dagster on every push | Auto-deploy after CI with a smoke test and a rollback to the previous image |
| **Secrets** | Gitignored files, separate passwords per environment | Azure Key Vault, plus the VM's managed identity for lake access, so no storage key sits on disk |

## The single catalog question (where this is going)

Stakeholders want one front door: what data exists, what it means, which
metrics are official, and whether today's numbers are healthy.

- **Today**, that's split across Dagster (health), dbt docs (meaning) and SQL (contents).
- **OpenMetadata** is the planned front door. It ingests the Postgres
  warehouse, dbt (descriptions, tests and lineage, down to columns) and
  pipeline metadata. It has a dedicated **Metrics** entity and a business
  glossary, and its built-in MCP server lets an AI assistant answer "where is
  X and can I trust it?".
- **Cube**, if added, would be the place metrics are *served* from, to
  dashboards, apps and the chatbot. It can generate its model from dbt's
  metadata (`cube_dbt`).
- **The gap:** OpenMetadata has no native Cube connector today, so Cube's
  metric definitions would be registered in OpenMetadata's Metrics catalog
  through its API. That's a small sync, kept in code.
- **The design rule:** define each metric once, serve it through one layer,
  and catalog it in one place.
