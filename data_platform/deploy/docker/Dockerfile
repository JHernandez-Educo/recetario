# One image for the whole platform: dlt connectors, dbt project, Dagster code.
FROM astral/uv:python3.12-trixie-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/home \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first (cached layer), project code second.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

# dbt manifest at build time, so Dagster can load the dbt assets without a dev step.
RUN dbt deps  --project-dir data_platform/transform --profiles-dir data_platform/transform \
 && dbt parse --project-dir data_platform/transform --profiles-dir data_platform/transform

COPY data_platform/deploy/dagster/dagster.yaml /opt/dagster/home/dagster.yaml
