"""Write-audit-publish with Iceberg branches.

    1. write   the export goes to a new branch, audit-<batch>, made from main.
               Readers of main (dashboards, DuckDB) can't see it.
    2. audit   run the quality checks against the branch's snapshot.
    3. publish fast-forward main to the branch head: one catalog commit,
               so readers go from "without the day" to "with the day" at once.
               If any check failed, main is never touched; the branch stays
               for someone to inspect (quarantine).

PyIceberg 0.12 has no fast-forward, so publish does it by hand: main may
only move to the branch head if main's current snapshot is an ancestor of
it. If someone committed to main after the branch was made, moving main to
the branch head would silently drop their commit; that is refused, and the
batch is staged again from the new main. The catalog's compare-and-swap
covers the gap between that check and the commit.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass, field
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table

from . import quality
from .ingest import BATCH_ID, DAY, conform, day_filter


class NotFastForward(Exception):
    pass


@dataclass
class WapResult:
    batch_id: str
    branch: str
    published: bool
    checks: list[quality.Check]
    restaged: int = 0
    seconds: dict[str, float] = field(default_factory=dict)

    @property
    def failed(self) -> list[quality.Check]:
        return [c for c in self.checks if not c.passed]


def is_ancestor(table: Table, ancestor_id: int | None, snapshot_id: int) -> bool:
    if ancestor_id is None:
        return True
    snap = table.snapshot_by_id(snapshot_id)
    while snap is not None:
        if snap.snapshot_id == ancestor_id:
            return True
        snap = table.snapshot_by_id(snap.parent_snapshot_id) if snap.parent_snapshot_id else None
    return False


def stage(table: Table, batch: pa.Table, day: dt.date, batch_id: str) -> tuple[str, int]:
    """Write the export to a fresh branch made from main. Returns the branch
    and the snapshot it was made from."""
    branch = f"audit-{batch_id}"
    table.refresh()
    if branch in table.refs():
        table.manage_snapshots().remove_branch(branch).commit()
    base = table.current_snapshot()
    if base is None:
        raise ValueError("write-audit-publish needs a table with at least one snapshot to branch from")
    # A branch must be created from main first: writing to a branch that
    # doesn't exist yet makes an *empty* one, without main's data.
    table.manage_snapshots().create_branch(base.snapshot_id, branch).commit()
    table.overwrite(conform(batch, table), overwrite_filter=day_filter(day),
                    snapshot_properties={BATCH_ID: batch_id, DAY: str(day)}, branch=branch)
    return branch, base.snapshot_id


def staged_rows(table: Table, branch: str, base: int) -> pa.Table:
    """Exactly the rows the branch added on top of main, wherever they landed
    (rows dated outside the day go to other partitions; the audit must see
    them too)."""
    return table.incremental_append_scan(from_snapshot_id_exclusive=base,
                                         to_snapshot_id_inclusive=table.refs()[branch].snapshot_id).to_arrow()


def history(table: Table, day: dt.date, n: int = 7) -> list[Decimal]:
    """Published daily totals of the n days before."""
    col = quality.cost_column(table.schema().column_names)
    start = day - dt.timedelta(days=n)
    t = table.scan(row_filter=f"usage_start >= '{start}T00:00:00' AND usage_start < '{day}T00:00:00'",
                   selected_fields=("usage_start", col)).to_arrow()
    if t.num_rows == 0:
        return []
    days = pc.floor_temporal(t["usage_start"], unit="day")
    out = t.append_column("d", days).group_by("d").aggregate([(col, "sum")]).sort_by("d")
    return out[f"{col}_sum"].to_pylist()


def publish(table: Table, branch: str) -> None:
    table.refresh()
    head = table.refs()[branch].snapshot_id
    main = table.current_snapshot().snapshot_id if table.current_snapshot() else None
    if not is_ancestor(table, main, head):
        raise NotFastForward(f"main moved since {branch} was made")
    try:
        # asserts main is still where we just saw it (compare-and-swap)
        table.manage_snapshots().set_current_snapshot(snapshot_id=head).commit()
    except CommitFailedException as e:
        raise NotFastForward(str(e)) from e
    table.manage_snapshots().remove_branch(branch).commit()


def load(table: Table, batch: pa.Table, day: dt.date, batch_id: str, control: quality.Control,
         attempts: int = 5) -> WapResult:
    restaged = 0
    for _ in range(attempts):
        timings = {}
        t = time.perf_counter()
        branch, base = stage(table, batch, day, batch_id)
        timings["write"] = time.perf_counter() - t
        t = time.perf_counter()
        checks = quality.run(staged_rows(table, branch, base), control, history(table, day))
        timings["audit"] = time.perf_counter() - t
        if not all(c.passed for c in checks):
            return WapResult(batch_id, branch, False, checks, restaged, timings)
        t = time.perf_counter()
        try:
            publish(table, branch)
        except NotFastForward:
            restaged += 1
            continue
        timings["publish"] = time.perf_counter() - t
        return WapResult(batch_id, branch, True, checks, restaged, timings)
    raise NotFastForward(f"gave up after {attempts} attempts")
