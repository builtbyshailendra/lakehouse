"""FinOps questions over the lakehouse, in DuckDB SQL on the Iceberg tables.

    showback(m)      cost per team per month, with month-over-month change
    spikes(m)        hours where a (team, service, region) group cost far more
                     than the same hour on the previous days, merged into events
    incident_cost()  spikes matched to the incident log: what each incident cost

The generator scaled usage up during 27 incidents, so the spike detector can
be scored against the truth (tests/test_analytics.py).
"""

from __future__ import annotations

import duckdb

from .config import duck

COST = "effective_cost"


def connect(cost_metadata: str, incidents_metadata: str, cost_col: str = COST) -> duckdb.DuckDBPyConnection:
    c = duck()
    c.execute(f"CREATE VIEW cost AS SELECT * RENAME ({cost_col} AS cost) FROM iceberg_scan('{cost_metadata}')")
    c.execute(f"CREATE VIEW incidents AS SELECT * FROM iceberg_scan('{incidents_metadata}')")
    return c


SHOWBACK = """
WITH m AS (
    SELECT team, date_trunc('month', usage_start)::DATE AS month, sum(cost) AS cost
    FROM cost GROUP BY ALL
)
SELECT team, month, cost,
       round(100 * (cost / lag(cost) OVER (PARTITION BY team ORDER BY month) - 1), 1) AS mom_pct
FROM m ORDER BY month, cost DESC
"""

# Hourly cost per group; baseline = median of the same hour on the same kind
# of day (weekday or weekend) over the previous 14 days, so the daily curve
# and the weekend dip don't look like spikes or hide them. A spike is
# >= threshold x baseline. Consecutive spike hours become one event
# (gaps-and-islands).
SPIKES = """
WITH hourly AS (
    SELECT team, service, region, usage_start AS hour, dayofweek(usage_start) IN (0, 6) AS weekend, sum(cost) AS cost
    FROM cost WHERE service IN ('compute', 'database', 'network')
    GROUP BY ALL
),
scored AS (
    SELECT *,
        median(cost) OVER w AS baseline,
        count(*) OVER w AS history
    FROM hourly
    WINDOW w AS (PARTITION BY team, service, region, hour(hour), weekend ORDER BY hour
                 RANGE BETWEEN INTERVAL 14 DAYS PRECEDING AND INTERVAL 1 DAY PRECEDING)
),
flagged AS (
    SELECT *, hour - to_hours(row_number() OVER (PARTITION BY team, service, region ORDER BY hour)::INT) AS island
    FROM scored WHERE history >= $min_history AND cost >= $threshold * baseline
)
SELECT team, service, region, min(hour) AS started_at, max(hour) + INTERVAL 1 HOUR AS ended_at,
       count(*) AS hours, round(max(cost / baseline), 2) AS peak_ratio,
       sum(cost - baseline) AS excess_cost
FROM flagged GROUP BY team, service, region, island
ORDER BY started_at
"""

INCIDENT_COST = """
SELECT i.incident_id, i.severity, i.team, i.service, i.region, i.started_at,
       s.started_at AS spike_started_at, s.hours, s.peak_ratio, round(s.excess_cost, 2) AS excess_cost
FROM incidents i LEFT JOIN spikes s
  ON s.team = i.team AND s.service = i.service AND s.region = i.region
 AND s.started_at < i.resolved_at AND s.ended_at > i.started_at
ORDER BY i.started_at
"""


def showback(c: duckdb.DuckDBPyConnection):
    return c.execute(SHOWBACK).fetchall()


def spikes(c: duckdb.DuckDBPyConnection, threshold: float = 1.8, min_history: int = 2) -> None:
    """Materialise the spikes as a temp table (used by incident_cost)."""
    c.execute("DROP TABLE IF EXISTS spikes")
    c.execute(f"CREATE TEMP TABLE spikes AS {SPIKES}", {"threshold": threshold, "min_history": min_history})


def incident_cost(c: duckdb.DuckDBPyConnection):
    return c.execute(INCIDENT_COST).fetchall()


def true_excess(c: duckdb.DuckDBPyConnection, scales: dict[str, float]) -> dict[str, float]:
    """What each incident really added: during it, the affected group's cost
    was scale x normal, so the excess is cost x (1 - 1/scale)."""
    out = {}
    for inc_id, team, service, region, start, end in c.execute(
            "SELECT incident_id, team, service, region, started_at, resolved_at FROM incidents").fetchall():
        cost = c.execute("SELECT sum(cost) FROM cost WHERE team = ? AND service = ? AND region = ? "
                         "AND usage_start >= ? AND usage_start < ?", [team, service, region, start, end]).fetchone()[0]
        out[inc_id] = float(cost or 0) * (1 - 1 / scales[inc_id])
    return out
