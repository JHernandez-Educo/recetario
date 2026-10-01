# data_platform

Data flows through three stages, in order:

| # | Folder | Tool | Job |
|---|---|---|---|
| 1 | [connectors/](connectors/) | dlt | Extract from each source and load raw tables |
| 2 | [transform/](transform/) | dbt | Clean, model (star schema), and test |
| 3 | [orchestration/](orchestration/) | Dagster | Decide what runs, when, and in which order |

[deploy/](deploy/) holds how the platform runs: container and database setup. No business logic lives there.

Layers: `raw_<source>` (as loaded by dlt) → `clean_<source>` (typed, deduplicated, one schema per source) → `marts` (`_DIM` / `_FACT` tables, where sources are unioned).
Naming conventions: [docs/dbt-conventions.md](../docs/dbt-conventions.md).
