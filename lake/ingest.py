"""Loading the daily billing export into the cost table.

A cloud bill isn't append-only: providers restate days (late usage, credits,
refunds) and resend the whole day. So a day is loaded by *replacing its
partition*: one Iceberg commit that removes whatever the table held for that
day and adds the new files. Loading the same export twice gives the same
table, which is what makes reruns and retries safe.
"""

from __future__ import annotations

import datetime as dt
import random
import time
from dataclasses import dataclass, field

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException, ValidationException
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
    conflicts: int = 0  # times another writer's commit got in the way
    conflict_errors: list[str] = field(default_factory=list)


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


def load_day(table: Table, batch: pa.Table, day: dt.date, batch_id: str | None = None, attempts: int = 10) -> LoadResult:
    """Replace the day's partition with this export, atomically.

    The batch id goes into the snapshot summary, so the table itself records
    which exports it holds; a batch already in main's history is skipped
    (the same trick Flink's Iceberg sink uses for exactly-once: the last
    committed checkpoint id lives in the snapshot summary).

    Concurrency: PyIceberg retries a commit that lost the compare-and-swap,
    re-validating against what landed in between. If another writer replaced
    the same day meanwhile, that validation fails (serializable isolation),
    and this loop starts over: re-read the table, check the batch id again,
    replace the day again. So racing loads of one day end with exactly one
    complete version of it, never two.
    """
    batch_id = batch_id or f"cost-{day}"
    data = conform(batch, table)
    conflicts = 0
    errors: list[str] = []
    for attempt in range(attempts):
        if batch_id in loaded_batches(table):
            snap = table.current_snapshot()
            return LoadResult(day, batch_id, 0, snap.snapshot_id if snap else None, skipped=True, conflicts=conflicts,
                              conflict_errors=errors)
        try:
            table.overwrite(
                data,
                overwrite_filter=day_filter(day),
                snapshot_properties={BATCH_ID: batch_id, DAY: str(day)},
            )
            return LoadResult(day, batch_id, data.num_rows, table.current_snapshot().snapshot_id, conflicts=conflicts,
                              conflict_errors=errors)
        except (CommitFailedException, ValidationException) as e:
            errors.append(f"{type(e).__name__}: {e}"[:200])
            if attempt == attempts - 1:
                raise
            conflicts += 1
            time.sleep(min(0.05 * 2**attempt, 2.0) * random.uniform(0.5, 1.5))
            table.refresh()
    raise AssertionError("unreachable")
