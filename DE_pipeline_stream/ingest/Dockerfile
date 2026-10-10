# ─────────────────────────────────────────────────────────────────────────────
# bridge image: source SSE stream → Kafka (package bikeshare-bridge).
#
# Same recipe as source/Dockerfile: repo root as the build context (for
# uv.lock), and only this workspace member plus its locked dependencies.
# confluent-kafka's wheel bundles librdkafka, so no system packages are needed.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Dependencies only (cached until uv.lock or a pyproject changes).
COPY pyproject.toml uv.lock ./
COPY source/pyproject.toml source/
COPY ingest/pyproject.toml ingest/
RUN uv sync --frozen --no-default-groups --package bikeshare-bridge --no-install-workspace

# 2) The code, installed non-editable into /opt/venv.
COPY ingest/ ingest/
RUN uv sync --frozen --no-default-groups --package bikeshare-bridge --no-editable

# data/bridge/ (the checkpoint) is mounted at runtime (see docker-compose.yml).
CMD ["python", "-m", "bikeshare_bridge"]
