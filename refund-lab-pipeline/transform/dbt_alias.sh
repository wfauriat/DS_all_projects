#!/usr/bin/env bash
## dbt command reference for this project.
## Use like alias.sh: "source transform/dbt_alias.sh" (works from any cwd —
## paths below are computed from this script's own location, not from where
## you happen to be standing when you source it).

# transform/, holds dbt_project.yml/profiles.yml
DBT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# repo root, holds pyproject.toml
DBT_PROJECT_DIR="$(dirname "$DBT_DIR")"

# `--project` tells `uv run` which .venv/pyproject to use (dbt-duckdb lives in
# the main project's deps, not a separate one for transform/). It does NOT
# tell dbt itself anything.
#
# Every dbt invocation below also runs inside "(cd "$DBT_DIR" && ...)" — a
# subshell, so it doesn't change your actual shell's directory. This turned
# out to be necessary, not just tidy: dbt's own --project-dir/--profiles-dir
# flags control where dbt looks for dbt_project.yml/profiles.yml, but the
# dbt-duckdb adapter resolves the *data file* paths inside profiles.yml
# ("path: warehouse.duckdb", "attach: ../landing.db") relative to the
# process's cwd regardless of those flags. Running from the wrong directory
# silently created a second, empty warehouse.duckdb instead of erroring —
# cd'ing into transform/ first is what actually fixes it.

# Shared prefix, factored out so the alias lines below stay short.
DBT_RUN="cd '$DBT_DIR' && uv run --project '$DBT_PROJECT_DIR'"
DBT_FLAGS="--profiles-dir '$DBT_DIR'"

alias dbtdebug="($DBT_RUN dbt debug $DBT_FLAGS)"
# Connection sanity check — run this first if anything else misbehaves.

alias dbtrun="($DBT_RUN dbt run $DBT_FLAGS)"
# (Re)builds every model as its configured materialization (view, by default
# for this project — see dbt_project.yml). Does NOT run tests.

alias dbttest="($DBT_RUN dbt test $DBT_FLAGS)"
# Runs the data_tests declared in schema.yml (unique, not_null, ...) against
# whatever's already built. Run dbtrun first if models changed.

alias dbtbuild="($DBT_RUN dbt build $DBT_FLAGS)"
# run + test together, in dependency order. The one to reach for day-to-day
# once you're not debugging a single model in isolation.

alias dbtclean="($DBT_RUN dbt clean $DBT_FLAGS)"
# Wipes transform/target/ (compiled SQL, run artifacts) for a fresh start.
# Does NOT touch warehouse.duckdb or landing.db.

# dbt show takes a SQL string (and optionally a row limit), so it has to be a
# function — aliases can't take arguments. Usage:
#   dbtshow "select * from {{ ref('orders_current') }}"
#   dbtshow "select * from {{ ref('orders_current') }}" 20
dbtshow() {
    local query="$1"
    local limit="${2:-5}"
    (
        cd "$DBT_DIR" && \
        uv run --project "$DBT_PROJECT_DIR" \
            dbt show --inline "$query" --limit "$limit" \
            --profiles-dir "$DBT_DIR"
    )
    # Note: never put LIMIT inside $query yourself — dbt show wraps your SQL
    # and appends its own LIMIT from the --limit flag; a literal LIMIT in the
    # query text collides with it and DuckDB's parser rejects the result.
}