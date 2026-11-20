"""Data-quality checks on one day's billing export.

Each check returns pass/fail with a reason. The control totals come from the
export's own manifest (cloud providers ship one next to the data: row count,
total), so a truncated or doubled transfer is caught even when every row
looks fine on its own.
"""

from __future__ import annotations

import datetime as dt
import statistics
from dataclasses import dataclass
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from . import gen


@dataclass(frozen=True)
class Control:
    """What the export's manifest file says it contains."""

    day: dt.date
    rows: int
    billed_total: Decimal


def control_for(batch: pa.Table, day: dt.date) -> Control:
    return Control(day, batch.num_rows, pc.sum(batch["billed_cost"]).as_py())


def cost_column(names) -> str:
    """effective_cost after the FOCUS rename (step 3), billed_cost before."""
    return "effective_cost" if "effective_cost" in names else "billed_cost"


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def run(day_rows: pa.Table, control: Control, history: list[Decimal]) -> list[Check]:
    """day_rows: what the staged branch holds for the day. history: the
    published daily totals of the days before (for the anomaly check)."""
    cost_col = cost_column(day_rows.column_names)
    c = duckdb.connect()
    c.register("d", day_rows)
    lo = dt.datetime.combine(control.day, dt.time())
    hi = lo + dt.timedelta(days=1)
    q = lambda sql: c.execute(sql).fetchone()  # noqa: E731
    rows, total, dupes, neg, over, outside, unknown = q(f"""
        SELECT count(*), sum({cost_col}),
               count(*) - count(DISTINCT line_id),
               count(*) FILTER (WHERE usage_quantity < 0),
               count(*) FILTER (WHERE {cost_col} > list_cost),
               count(*) FILTER (WHERE usage_start < '{lo}' OR usage_start >= '{hi}'),
               count(*) FILTER (WHERE team NOT IN ({", ".join(f"'{t}'" for t in gen.TEAMS)}))
        FROM d""")
    total = total or Decimal(0)
    checks = [
        Check("row count matches the export manifest", rows == control.rows, f"{rows:,} rows, manifest says {control.rows:,}"),
        Check("billed total matches the export manifest", total == control.billed_total,
              f"{total} vs {control.billed_total}"),
        Check("line ids unique", dupes == 0, f"{dupes:,} duplicates"),
        Check("no negative usage", neg == 0, f"{neg:,} rows"),
        Check("billed never above list price", over == 0, f"{over:,} rows"),
        Check("every row inside the day", outside == 0, f"{outside:,} rows outside {control.day}"),
        Check("every team known", unknown == 0, f"{unknown:,} rows with an unknown team"),
    ]
    if len(history) >= 3:
        base = statistics.median(history[-7:])
        ratio = float(total / base) if base else float("inf")
        checks.append(Check("total within 0.5x-2x of the last 7 days' median", 0.5 <= ratio <= 2.0,
                            f"{ratio:.2f}x the median ({base:,.2f})"))
    return checks
