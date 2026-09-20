# ─────────────────────────────────────────────────────────────────────────────
# source-api image: the synthetic bike-share operator (package bikeshare-sim).
#
# Built by docker-compose.yml (service source-api) with the REPO ROOT as the
# build context, because the workspace lockfile (uv.lock) lives there. Only this
# member and its locked dependencies are installed (`uv sync --package`), so
# the container runs exactly the versions the tests ran against.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# uv, from its official image. Same version as the one that wrote uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Dependencies only. This layer is cached until uv.lock or a pyproject changes.
#    uv needs every member's pyproject to understand the workspace.
COPY pyproject.toml uv.lock ./
COPY source/pyproject.toml source/
COPY ingest/pyproject.toml ingest/
RUN uv sync --frozen --no-default-groups --package bikeshare-sim --no-install-workspace

# 2) The code, installed as a regular (non-editable) package into /opt/venv.
COPY source/ source/
RUN uv sync --frozen --no-default-groups --package bikeshare-sim --no-editable

# config/ and data/source/ are mounted at runtime (see docker-compose.yml).
EXPOSE 8000
CMD ["python", "-m", "bikeshare_sim"]
