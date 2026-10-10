"""A Streamlit dashboard on the serving copy: detection, data quality, the city, the pipeline.

    make dashboard      → http://localhost:8501      (= uv run streamlit run dashboard/app.py)

It reads data/warehouse/bikeshare_serving.duckdb: the marts Airflow republishes after
every successful dbt build (DAG dbt_transform → task publish_serving). Never the
warehouse itself: DuckDB allows one writer, and a dashboard must not queue behind a
landing, nor make one wait.

Each publish REPLACES the file (os.replace), which shapes how the page reads it:
  - the marts are loaded once per version of the file: the cache key is its mtime;
  - a connection opened on the old file keeps reading the old version, never a mix;
  - a fragment looks at the file every 30 s and reruns the page when a new copy lands.

Colors: three categorical slots validated for color-vision deficiencies in light and
dark mode (bridge, spark and dbt keep their slot in every chart), and one blue ramp
for magnitudes. Every chart has tooltips, and a table view below it.
"""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

SERVING = Path(os.environ.get("SERVING_PATH", "data/warehouse/bikeshare_serving.duckdb"))

# ── Colors ──────────────────────────────────────────────────────────────────
# Slots 1–3 of the reference palette, each with its light and its dark step.
SERIES = {"light": ["#2a78d6", "#eb6834", "#1baf7a"], "dark": ["#3987e5", "#d95926", "#199e70"]}
# Low → high. Every station must stay visible, so the light ramp starts at step 250, not 100.
SEQUENTIAL = {"light": ["#86b6ef", "#0d366b"], "dark": ["#184f95", "#cde2fb"]}
SURFACE = {"light": "#ffffff", "dark": "#0e1117"}  # Streamlit's page: the gaps and rings
MUTED = "#898781"

DETECTORS = ["bridge", "spark", "dbt"]  # pipeline order = color order
FAULTS = [
    "schema_drift",
    "impossible_status",
    "duplicate",
    "late_event",
    "stream_stall",
    "teleport",
    "orphan_trip",
    "frozen_station",
    "silent_station",
]
VERDICTS = ["true detection", "explained by another fault", "unexplained"]
DAY_AXIS = alt.Axis(format="%b %d", labelAngle=0, tickCount=8)
HOUR_AXIS = alt.Axis(format="%H:%M", labelAngle=0)
PERCENT = st.column_config.NumberColumn(format="percent")
DAY = st.column_config.DateColumn(format="MMM DD")

# ── Data ────────────────────────────────────────────────────────────────────
# Shaped for the charts. Sums are cast to BIGINT: DuckDB's HUGEINT reaches pandas as float.
QUERIES = {
    "published": "SELECT published_at FROM main.published",
    "scorecard": "SELECT * FROM marts.mart_detection_scorecard",
    "quality": "SELECT * FROM marts.mart_data_quality_daily ORDER BY day",
    "trips": """
        SELECT trip_date, hour(started_at) AS hour, rider_type, bike_type, duration_s,
               is_implausible_speed
        FROM marts.fct_trips""",
    "stations": """
        SELECT s.station_id, s.name, s.zone, s.lat, s.lon, s.capacity, u.hour::DATE AS day,
               sum(u.departures)::BIGINT AS departures, sum(u.arrivals)::BIGINT AS arrivals,
               sum(u.minutes_empty)::BIGINT AS minutes_empty,
               sum(u.minutes_full)::BIGINT AS minutes_full, count(*) AS hours
        FROM marts.mart_station_usage_hourly u JOIN marts.dim_stations s USING (station_id)
        GROUP BY ALL""",
    "health": """
        SELECT landed_hour, batches, rows_landed::BIGINT AS rows_landed,
               latency_p50_s, latency_p95_s, latency_max_s
        FROM marts.mart_pipeline_health ORDER BY landed_hour""",
    "stream_vs_batch": """
        SELECT day, windows, batch_trip_events::BIGINT AS batch_trip_events,
               stream_trip_events::BIGINT AS stream_trip_events,
               missing_in_stream::BIGINT AS missing_in_stream, windows_that_differ
        FROM marts.mart_stream_vs_batch ORDER BY day""",
}


@st.cache_data(show_spinner="Reading the serving copy…")
def load(path: str, mtime: float) -> dict[str, pd.DataFrame]:
    """Every query, once per published version of the file (`mtime` is part of the cache key)."""
    with duckdb.connect(path, read_only=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        return {name: conn.execute(sql).df() for name, sql in QUERIES.items()}


@st.fragment(run_every=timedelta(seconds=30))
def follow_new_copies(loaded_mtime: float) -> None:
    """Rerun the whole page once Airflow has published a new serving copy."""
    if SERVING.exists() and SERVING.stat().st_mtime != loaded_mtime:
        st.rerun(scope="app")


# ── Chart helpers: thin marks, hairline chrome, a tooltip on every mark ─────
def mode() -> str:
    return "dark" if st.context.theme.type == "dark" else "light"


def categorical(domain: list[str]) -> alt.Scale:
    """A slot per position in the FULL domain: an entity keeps its color whatever is shown."""
    return alt.Scale(domain=domain, range=SERIES[mode()][: len(domain)])


def top_legend() -> alt.Legend:
    return alt.Legend(orient="top", title=None, symbolType="circle")


def show(chart: alt.TopLevelMixin) -> None:
    st.altair_chart(chart, width="stretch", theme="streamlit")


def line_chart(
    df: pd.DataFrame, x: str, x_enc: alt.X, y: str, series: str, domain: list[str], tooltip: list
) -> alt.LayerChart:
    """Lines with a crosshair: hovering highlights the nearest point and shows its values."""
    near = alt.selection_point(
        fields=[x], nearest=True, on="pointerover", clear="pointerout", empty=False
    )
    base = alt.Chart(df).encode(
        x=x_enc,
        y=alt.Y(f"{y}:Q", title=None),
        color=alt.Color(f"{series}:N", scale=categorical(domain), legend=top_legend()),
    )
    return alt.layer(
        base.mark_line(strokeWidth=2, strokeCap="round", strokeJoin="round"),
        alt.Chart(df).mark_rule(color=MUTED, strokeWidth=1).encode(x=x_enc).transform_filter(near),
        base.mark_point(
            filled=True, size=64, opacity=1, stroke=SURFACE[mode()], strokeWidth=2
        ).transform_filter(near),
        base.mark_point(size=400, opacity=0).encode(tooltip=tooltip).add_params(near),
    )


def column_chart(df: pd.DataFrame, x_enc: alt.X, y: str, tooltip: list) -> alt.Chart:
    """A single series: slot 1, thin columns rounded at the data end."""
    return (
        alt.Chart(df)
        .mark_bar(color=SERIES[mode()][0], size=12, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(x=x_enc, y=alt.Y(f"{y}:Q", title=None), tooltip=tooltip)
    )


def table_view(label: str, df: pd.DataFrame, **column_config) -> None:
    with st.expander(f"Table view: {label}"):
        st.dataframe(df, hide_index=True, width="stretch", column_config=column_config)


def kpis(*tiles: tuple[str, str]) -> None:
    for col, (label, value) in zip(st.columns(len(tiles)), tiles, strict=True):
        col.metric(label, value, border=True)


# ── Tabs ────────────────────────────────────────────────────────────────────
def detection_tab(scorecard: pd.DataFrame) -> None:
    st.caption(
        "Every finding of the bridge, Spark and dbt, matched to the source's ground truth "
        "(`marts.mart_detection_scorecard`). Only faults older than 4 simulated hours, and "
        "already in the extracted fault log, are judged. Not scoped by the simulated days above."
    )
    anyone = scorecard[scorecard.detector == "any"]
    checks = scorecard[scorecard.detector != "any"]
    per_check = checks.drop_duplicates(["detector", "check_name"])  # precision is per check
    judged, found = int(anyone.faults_judged.sum()), int(anyone.faults_found.sum())
    kpis(
        ("Faults judged", f"{judged:,}"),
        ("Found by at least one check", f"{found / judged:.1%}" if judged else "–"),
        ("Detections", f"{int(per_check.detections.sum()):,}"),
        ("Checks", f"{len(per_check)}"),
    )

    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Recall:** the share of injected faults each detector found")
        recall = checks.assign(
            found=checks.faults_found.astype(str) + " of " + checks.faults_judged.astype(str)
        )
        chart = (
            alt.Chart(recall)
            .mark_bar(size=8, cornerRadiusEnd=4)
            .encode(
                x=alt.X(
                    "recall:Q",
                    title=None,
                    scale=alt.Scale(domain=[0, 1]),
                    axis=alt.Axis(format="%", tickCount=5),
                ),
                y=alt.Y("fault_type:N", sort=FAULTS, title=None),
                yOffset=alt.YOffset("detector:N", sort=DETECTORS),
                color=alt.Color("detector:N", scale=categorical(DETECTORS), legend=top_legend()),
                tooltip=[
                    "fault_type",
                    "detector",
                    "check_name",
                    "found",
                    alt.Tooltip("recall:Q", format=".1%"),
                ],
            )
        )
        show(chart.properties(height=len(FAULTS) * 34))
    with right:
        st.markdown("**Precision:** what each check's detections point at")
        verdicts = per_check.assign(
            check=per_check.detector + " · " + per_check.check_name,
            unexplained=per_check.detections
            - per_check.true_detections
            - per_check.explained_by_other_faults,
            rank=per_check.detector.map({d: i for i, d in enumerate(DETECTORS)}),
        ).rename(columns={"true_detections": VERDICTS[0], "explained_by_other_faults": VERDICTS[1]})
        order = verdicts.sort_values(["rank", "check_name"]).check.tolist()
        long = verdicts.melt(
            id_vars=["check", "detections"], value_vars=VERDICTS, var_name="verdict", value_name="n"
        )
        long["stack"] = long.verdict.map({v: i for i, v in enumerate(VERDICTS)})
        long["share"] = long.n / long.detections.where(long.detections > 0)
        chart = (
            alt.Chart(long)
            .mark_bar(size=14, stroke=SURFACE[mode()], strokeWidth=2)  # the 2px surface gaps
            .encode(
                x=alt.X(
                    "n:Q", stack="normalize", title=None, axis=alt.Axis(format="%", tickCount=5)
                ),
                y=alt.Y("check:N", sort=order, title=None),
                color=alt.Color("verdict:N", scale=categorical(VERDICTS), legend=top_legend()),
                order=alt.Order("stack:Q"),
                tooltip=[
                    "check",
                    "verdict",
                    alt.Tooltip("n:Q", format=","),
                    alt.Tooltip("share:Q", format=".1%"),
                    alt.Tooltip("detections:Q", format=","),
                ],
            )
        )
        show(chart.properties(height=len(order) * 30))
    table_view(
        "scorecard",
        scorecard.sort_values(["fault_type", "detector"]),
        precision=PERCENT,
        recall=PERCENT,
    )


def quality_tab(quality: pd.DataFrame) -> None:
    st.caption(
        "Per simulated day (`marts.mart_data_quality_daily`). The first day starts at 05:00, "
        "and the last one is still under way."
    )
    kpis(
        ("Events", f"{int(quality.events.sum()):,}"),
        ("Duplicated by the source", f"{int(quality.duplicated_by_source.sum()):,}"),
        ("Re-sent by the bridge", f"{int(quality.resent_by_bridge.sum()):,}"),
        ("Late (≥ 25 min)", f"{int(quality.late_events.sum()):,}"),
        ("Dead-lettered", f"{int(quality.dead_letters.sum()):,}"),
    )
    day = alt.X("day:T", title=None, axis=DAY_AXIS)
    day_tip = alt.Tooltip("day:T", format="%b %d")
    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Events per simulated day**")
        tip = [day_tip, alt.Tooltip("events:Q", format=",")]
        show(column_chart(quality, day, "events", tip).properties(height=220))
    with right:
        st.markdown("**Data issues per simulated day**")
        issues = {
            "duplicated_by_source": "duplicated by the source",
            "late_events": "late (≥ 25 min)",
            "dead_letters": "dead-lettered",
        }
        long = quality.melt(
            id_vars=["day"], value_vars=list(issues), var_name="issue", value_name="count"
        )
        long["issue"] = long.issue.map(issues)
        tip = [day_tip, "issue", alt.Tooltip("count:Q", format=",")]
        chart = line_chart(long, "day", day, "count", "issue", list(issues.values()), tip)
        show(chart.properties(height=220))
    table_view("data quality per simulated day", quality, day=DAY)


def city_tab(trips: pd.DataFrame, stations: pd.DataFrame) -> None:
    st.caption(
        "The business side of the same data: completed trips (`marts.fct_trips`) and station "
        "activity (`marts.mart_station_usage_hourly`)."
    )
    kpis(
        ("Completed trips", f"{len(trips):,}"),
        ("Median trip", f"{trips.duration_s.median() / 60:.0f} min" if len(trips) else "–"),
        (
            "On an electric bike",
            f"{(trips.bike_type == 'electric').mean():.0%}" if len(trips) else "–",
        ),
        ("Implausible speed (teleports)", f"{int(trips.is_implausible_speed.sum()):,}"),
    )
    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Trips started per hour of day**, average per simulated day")
        days = max(trips.trip_date.nunique(), 1)
        by_hour = (
            trips.groupby(["hour", "rider_type"]).size().div(days).rename("trips").reset_index()
        )
        hour = alt.X(
            "hour:Q",
            title="hour of day",
            scale=alt.Scale(domain=[0, 23]),
            axis=alt.Axis(tickCount=12),
        )
        tip = ["hour", "rider_type", alt.Tooltip("trips:Q", format=".1f")]
        chart = line_chart(by_hour, "hour", hour, "trips", "rider_type", ["member", "casual"], tip)
        show(chart.properties(height=300))
    per_station = stations.groupby(
        ["station_id", "name", "zone", "lat", "lon", "capacity"], as_index=False
    ).agg({c: "sum" for c in ["departures", "arrivals", "minutes_empty", "minutes_full", "hours"]})
    per_station["empty_or_full"] = (per_station.minutes_empty + per_station.minutes_full) / (
        per_station.hours * 60
    ).where(per_station.hours > 0)
    per_station = per_station.sort_values("empty_or_full", ascending=False)
    with right:
        st.markdown("**Stations:** size is docks, color is the share of time empty or full")
        chart = (
            alt.Chart(per_station)
            .mark_circle(opacity=1, stroke=SURFACE[mode()], strokeWidth=2)  # the 2px surface ring
            .encode(
                x=alt.X("lon:Q", scale=alt.Scale(zero=False, padding=16), axis=None),
                y=alt.Y("lat:Q", scale=alt.Scale(zero=False, padding=16), axis=None),
                size=alt.Size(
                    "capacity:Q",
                    scale=alt.Scale(range=[60, 520]),
                    legend=alt.Legend(title="docks", orient="bottom", values=[20, 35, 50]),
                ),
                color=alt.Color(
                    "empty_or_full:Q",
                    scale=alt.Scale(range=SEQUENTIAL[mode()]),
                    legend=alt.Legend(title="empty or full", format="%", orient="bottom"),
                ),
                order=alt.Order("capacity:Q", sort="descending"),  # big circles behind small ones
                tooltip=[
                    "name",
                    "zone",
                    "capacity",
                    alt.Tooltip("departures:Q", format=","),
                    alt.Tooltip("arrivals:Q", format=","),
                    alt.Tooltip("empty_or_full:Q", format=".1%"),
                ],
            )
        )
        show(chart.properties(height=300))
    table_view(
        "stations, most often empty or full first",
        per_station.drop(columns=["lat", "lon"]),
        empty_or_full=PERCENT,
    )


def pipeline_tab(health: pd.DataFrame, stream_vs_batch: pd.DataFrame, published_at) -> None:
    st.caption(
        "The pipeline watching itself, in wall-clock time (`marts.mart_pipeline_health`), and "
        "Spark's real-time windows checked against the batch record "
        "(`marts.mart_stream_vs_batch`). Not scoped by the simulated days above."
    )
    latest = health.iloc[-1] if len(health) else None
    age_min = (datetime.now(UTC) - published_at.to_pydatetime()).total_seconds() / 60
    agree = int((stream_vs_batch.windows_that_differ == 0).sum())
    kpis(
        ("Serving copy published", f"{age_min:.0f} min ago"),
        ("Rows landed this hour", f"{int(latest.rows_landed):,}" if latest is not None else "–"),
        ("Latency p50 this hour", f"{latest.latency_p50_s:,.0f} s" if latest is not None else "–"),
        ("Days where Spark and batch agree", f"{agree} of {len(stream_vs_batch)}"),
    )
    # A utc scale: Vega-Lite would otherwise print these instants in the viewer's time zone.
    landed = alt.X("landed_hour:T", title=None, axis=HOUR_AXIS, scale=alt.Scale(type="utc"))
    hour_tip = alt.Tooltip("landed_hour:T", format="%b %d %H:%M", formatType="utc")
    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Rows landed per hour** (UTC)")
        tip = [hour_tip, "batches", alt.Tooltip("rows_landed:Q", format=",")]
        show(column_chart(health, landed, "rows_landed", tip).properties(height=220))
    with right:
        st.markdown("**Latency from the bridge to DuckDB**, seconds per event")
        names = {"latency_p50_s": "median", "latency_p95_s": "95th percentile"}
        long = health.melt(
            id_vars=["landed_hour"],
            value_vars=list(names),
            var_name="percentile",
            value_name="seconds",
        )
        long["percentile"] = long.percentile.map(names)
        tip = [hour_tip, "percentile", alt.Tooltip("seconds:Q", format=",.0f")]
        chart = line_chart(
            long, "landed_hour", landed, "seconds", "percentile", list(names.values()), tip
        )
        show(chart.properties(height=220))
        st.caption(
            "Landing runs every 5 minutes, so events wait up to about 300 s. "
            "An hour that follows a stopped stack shows the downtime."
        )
    # Mostly zeros: the days that differ are a short table, not a chart.
    differ = stream_vs_batch[stream_vs_batch.windows_that_differ > 0]
    st.markdown("**Days where Spark's windows and the batch record differ**")
    if differ.empty:
        st.caption("None: every compared window holds the same trip events on both sides.")
    else:
        st.dataframe(differ, hide_index=True, width="stretch", column_config={"day": DAY})
        st.caption(
            "`missing_in_stream`: trip events the batch record holds and Spark's windows don't, "
            "because they reached Spark behind its watermark (late events, stalls)."
        )
    table_view("pipeline health per hour", health)
    table_view("Spark vs batch per simulated day", stream_vs_batch, day=DAY)


# ── Page ────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Bike-share pipeline", page_icon="🚲", layout="wide")
st.title("Bike-share pipeline")

if not SERVING.exists():
    st.info(
        f"No serving copy yet at `{SERVING}`. The first successful `dbt_transform` run publishes "
        "it: `make up`, then wait for a landing, or run `make smoke`."
    )
    st.stop()

mtime = SERVING.stat().st_mtime
data = load(str(SERVING), mtime)
follow_new_copies(mtime)
quality = data["quality"]
if quality.empty:
    st.info("The serving copy holds no simulated day yet: wait for the next dbt run.")
    st.stop()

published_at = data["published"].published_at.iloc[0]
st.caption(
    f"The marts of the last successful dbt run, from the serving copy published "
    f"{published_at:%b %d, %H:%M} UTC. The page reloads by itself when a new copy lands."
)

# One filter row, above everything it scopes.
first, last = quality.day.min().date(), quality.day.max().date()
start, end = first, last
if first < last:
    start, end = st.slider(
        "Simulated days (Data quality and City)", first, last, (first, last), format="MMM DD"
    )
trips, stations = data["trips"], data["stations"]

detection, data_quality, city, pipeline = st.tabs(["Detection", "Data quality", "City", "Pipeline"])
with detection:
    detection_tab(data["scorecard"])
with data_quality:
    quality_tab(quality[quality.day.dt.date.between(start, end)])
with city:
    city_tab(
        trips[trips.trip_date.dt.date.between(start, end)],
        stations[stations.day.dt.date.between(start, end)],
    )
with pipeline:
    pipeline_tab(data["health"], data["stream_vs_batch"], published_at)
