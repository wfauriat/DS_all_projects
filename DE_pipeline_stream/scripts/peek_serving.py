"""Read the serving copy: the marts, as analysts, notebooks and dashboards should see them.

    make scorecard | make quality | make serving     (= uv run python scripts/peek_serving.py <view>)

It opens data/warehouse/bikeshare_serving.duckdb, never the warehouse itself, so
it is never blocked by a DAG writing. The copy is republished after each
successful dbt build (DAG dbt_transform → publish_serving).
"""

import os
import sys

import duckdb

SERVING = "data/warehouse/bikeshare_serving.duckdb"

VIEWS = {
    "published": """
        SELECT published_at::TIMESTAMP(0) AS published_at_utc, t.key AS table_name, t.value::BIGINT AS rows
        FROM main.published, json_each(tables) AS t ORDER BY table_name""",
    "scorecard": """
        SELECT fault_type, detector, check_name, detections, true_detections AS true,
               explained_by_other_faults AS explained, precision, faults_judged AS faults,
               faults_found AS found, recall
        FROM marts.mart_detection_scorecard""",
    "quality": "SELECT * FROM marts.mart_data_quality_daily ORDER BY day DESC LIMIT 10",
}


def main() -> None:
    view = sys.argv[1] if len(sys.argv) > 1 else "published"
    if not os.path.exists(SERVING):
        print(f"{SERVING} does not exist yet: the first dbt_transform run publishes it")
        return
    conn = duckdb.connect(SERVING, read_only=True)
    conn.execute("SET TimeZone = 'UTC'")
    conn.sql(VIEWS[view]).show(max_rows=60, max_width=250)


if __name__ == "__main__":
    main()
