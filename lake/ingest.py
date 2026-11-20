"""Loading the daily billing export into the cost table.

A cloud bill isn't append-only: providers restate days (late usage, credits,
refunds) and resend the whole day. So a day is loaded by *replacing its
partition*: one Iceberg commit that removes whatever the table held for that
day and adds the new files. Loading the same export twice gives the same
table, which is what makes reruns and retries safe.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.expressions import And, BooleanExpression, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table

BATCH_ID = "lake.batch-id"
DAY = "lake.day"


def day_filter(day: dt.date) -> BooleanExpression:
    nxt = day + dt.timedelta(days=1)
    return And(GreaterThanOrEqual("usage_start", f"{day}T00:00:00"), LessThan("usage_start", f"{nxt}T00:00:00"))


def conform(batch: pa.Table, table: Table) -> pa.Table:
    """Cast to the table's Arrow schema (names, types, required fields)."""
    return batch.select(table.schema().column_names).cast(table.schema().as_arrow())


@dataclass
class LoadResult:
    day: dt.date
    batch_id: str
    rows: int
    snapshot_id: int | None
    skipped: bool = False


def loaded_batches(table: Table) -> set[str]:
    """Batch ids recorded on the snapshots in main's history."""
    out = set()
    snap = table.current_snapshot()
    while snap is not None:
        bid = snap.summary.additional_properties.get(BATCH_ID) if snap.summary else None
        if bid:
            out.add(bid)
        snap = table.snapshot_by_id(snap.parent_snapshot_id) if snap.parent_snapshot_id else None
    return out


def load_day(table: Table, batch: pa.Table, day: dt.date, batch_id: str | None = None) -> LoadResult:
    """Replace the day's partition with this export, atomically.

    The batch id goes into the snapshot summary, so the table itself records
    which exports it holds; a batch already in main's history is skipped.
    """
    batch_id = batch_id or f"cost-{day}"
    if batch_id in loaded_batches(table):
        return LoadResult(day, batch_id, 0, table.current_snapshot().snapshot_id, skipped=True)
    table.overwrite(
        conform(batch, table),
        overwrite_filter=day_filter(day),
        snapshot_properties={BATCH_ID: batch_id, DAY: str(day)},
    )
    return LoadResult(day, batch_id, batch.num_rows, table.current_snapshot().snapshot_id)
