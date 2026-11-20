"""Synthetic cloud billing and incident data, deterministic.

A fictional company with 4,000 cloud resources owned by 8 teams. Every day
the "billing export" delivers one line item per resource per hour (one per
day for storage), in the spirit of the FinOps FOCUS format: about 77,600
rows a day. Incidents make a team's compute, database or network usage scale
out for a few hours, so they show up as cost spikes that the analytics step
has to find again.

Nothing here is real: names, numbers and incidents are all made up.
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass

import duckdb
import pyarrow as pa

START = dt.date(2026, 8, 1)
DAYS = 92  # 1 Aug to 31 Oct 2026
N_RESOURCES = 4000
SEED = 20261120

TEAMS = ["payments", "search", "checkout", "identity", "data-platform", "ml-platform", "growth", "observability"]
REGIONS = ["us-east-1", "eu-west-1", "ap-south-1", "us-west-2"]
# service: (share of resources, unit, list price per unit, hourly rows?, size range)
SERVICES = {
    "compute": (0.45, "vCPU-hours", 0.04, True, (2, 64)),
    "database": (0.15, "ACU-hours", 0.12, True, (2, 32)),
    "storage": (0.20, "GB-months", 0.023, False, (100, 50_000)),
    "network": (0.10, "GB", 0.09, True, (1, 200)),
    "ml": (0.04, "GPU-hours", 2.50, True, (1, 8)),
    "observability": (0.06, "GB", 0.50, True, (1, 20)),
}
# Committed-use discount on production compute and database.
DISCOUNT = 0.72


@dataclass(frozen=True)
class Incident:
    incident_id: str
    team: str
    service: str
    region: str
    severity: str
    started_at: dt.datetime
    resolved_at: dt.datetime
    scale: float  # usage multiplier on the affected resources while it lasts


def resources() -> pa.Table:
    rng = random.Random(SEED)
    rows = {k: [] for k in ["r", "resource_id", "team", "account_id", "service", "region", "unit", "rate", "size", "hourly", "prod"]}
    names = list(SERVICES)
    weights = [SERVICES[s][0] for s in names]
    for i in range(N_RESOURCES):
        team = rng.choice(TEAMS)
        service = rng.choices(names, weights)[0]
        _, unit, rate, hourly, (lo, hi) = SERVICES[service]
        prod = rng.random() < 0.7
        rows["r"].append(i)
        rows["resource_id"].append(f"r-{i:05d}")
        rows["team"].append(team)
        rows["account_id"].append(f"{team}-{'prod' if prod else 'staging'}")
        rows["service"].append(service)
        rows["region"].append(rng.choice(REGIONS))
        rows["unit"].append(unit)
        rows["rate"].append(rate)
        # log-uniform sizes: many small resources, a few big ones
        rows["size"].append(round(lo * (hi / lo) ** rng.random(), 3))
        rows["hourly"].append(hourly)
        rows["prod"].append(prod)
    return pa.table(rows)


def incidents() -> list[Incident]:
    """About two incidents a week, on services that scale out under load."""
    rng = random.Random(SEED + 1)
    out = []
    day = 0
    n = 0
    while True:
        day += rng.randint(1, 6)
        if day >= DAYS:
            return out
        n += 1
        start = dt.datetime.combine(START + dt.timedelta(days=day), dt.time(rng.randint(0, 20)))
        hours = rng.randint(1, 3)
        sev = rng.choices(["SEV1", "SEV2", "SEV3"], [0.15, 0.35, 0.5])[0]
        scale = {"SEV1": 5.0, "SEV2": 3.5, "SEV3": 2.5}[sev]
        out.append(Incident(
            incident_id=f"INC-{n:04d}",
            team=rng.choice(TEAMS),
            service=rng.choice(["compute", "database", "network"]),
            region=rng.choice(REGIONS),
            severity=sev,
            started_at=start,
            resolved_at=start + dt.timedelta(hours=hours),
            scale=scale,
        ))


_con: duckdb.DuckDBPyConnection | None = None


def _duck() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = duckdb.connect()
        _con.register("resources", resources())
        inc = incidents()
        _con.register("incidents", pa.table({
            "team": [i.team for i in inc], "service": [i.service for i in inc], "region": [i.region for i in inc],
            "started_at": pa.array([i.started_at for i in inc], pa.timestamp("us")),
            "resolved_at": pa.array([i.resolved_at for i in inc], pa.timestamp("us")),
            "scale": [i.scale for i in inc],
        }))
    return _con


# One row per hourly resource per hour, one per storage resource per day.
# Usage follows a daily curve (peak mid-afternoon UTC), drops 20% at the
# weekend, grows 0.3% a day, and carries +-10% noise from a hash, so the same
# day always produces the same rows.
_DAY_SQL = """
WITH hours AS (
    SELECT h, $day::TIMESTAMP + to_hours(h) AS usage_start FROM range(24) t(h)
),
lines AS (
    SELECT r.*, hours.h, hours.usage_start
    FROM resources r JOIN hours ON r.hourly OR hours.h = 0
),
usage AS (
    SELECT l.*,
        CASE WHEN l.hourly
             THEN l.size * (1 + 0.35 * sin(2 * pi() * (l.h - 8) / 24))
                  * CASE WHEN dayofweek(l.usage_start) IN (0, 6) THEN 0.8 ELSE 1 END
             ELSE l.size / 30.0 END
        * (1 + 0.003 * $day_index)
        * (0.9 + 0.2 * ((hash(l.r, l.usage_start) % 1000003) / 1000003.0))
        * coalesce((SELECT max(i.scale) FROM incidents i
                    WHERE i.team = l.team AND i.service = l.service AND i.region = l.region
                      AND l.usage_start >= i.started_at AND l.usage_start < i.resolved_at), 1.0)
        AS qty
    FROM lines l
)
SELECT
    resource_id || '@' || strftime(usage_start, '%Y-%m-%dT%H') AS line_id,
    usage_start,
    account_id, team, service, region, resource_id,
    service || ':' || unit AS usage_type,
    round(qty, 6) AS usage_quantity,
    unit,
    CAST(round(qty, 6) * rate AS DECIMAL(14, 6)) AS list_cost,
    CAST(round(qty, 6) * rate * CASE WHEN prod AND service IN ('compute', 'database') THEN $discount ELSE 1 END
         AS DECIMAL(14, 6)) AS billed_cost
FROM usage
ORDER BY usage_start, resource_id
"""


def day_batch(day: dt.date) -> pa.Table:
    """The billing export for one day."""
    idx = (day - START).days
    return _duck().sql(_DAY_SQL, params={"day": day, "day_index": idx, "discount": DISCOUNT}).to_arrow_table()


def incident_table() -> pa.Table:
    inc = incidents()
    return pa.table({
        "incident_id": [i.incident_id for i in inc],
        "team": [i.team for i in inc],
        "service": [i.service for i in inc],
        "region": [i.region for i in inc],
        "severity": [i.severity for i in inc],
        "started_at": pa.array([i.started_at for i in inc], pa.timestamp("us")),
        "resolved_at": pa.array([i.resolved_at for i in inc], pa.timestamp("us")),
    })


def days(n: int = DAYS, start: dt.date = START) -> list[dt.date]:
    return [start + dt.timedelta(days=i) for i in range(n)]


def restate(batch: pa.Table, factor: float, list_too: bool = False) -> pa.Table:
    """The same export with every billed cost scaled, e.g. 0.9 for a 10%
    credit applied after the fact (list_too: the list price as well, like a
    pricing bug on the provider's side). Rounded back to 6 decimals."""
    con = duckdb.connect()
    con.register("b", batch)
    f = repr(float(factor))
    cols = [f"CAST(round(billed_cost * {f}, 6) AS DECIMAL(14, 6)) AS billed_cost"]
    if list_too:
        cols.append(f"CAST(round(list_cost * {f}, 6) AS DECIMAL(14, 6)) AS list_cost")
    return con.sql(f"SELECT * REPLACE ({', '.join(cols)}) FROM b").to_arrow_table()
