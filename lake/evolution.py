"""Changing a table that already holds data: schema, partitioning, history.

Every change here is a metadata commit. No data file is rewritten: old files
keep their old column names and their old partition layout, and readers map
them through field IDs and partition spec IDs.
"""

from __future__ import annotations

import datetime as dt

from pyiceberg.table import Table
from pyiceberg.types import StringType

from .ingest import BATCH_ID, DAY


def adopt_focus_names(table: Table) -> None:
    """The FinOps FOCUS 1.0 spec calls the amount actually paid
    EffectiveCost, and records which commitment (savings plan, reservation)
    it came from. Rename one column, add the other."""
    with table.update_schema() as u:
        u.rename_column("billed_cost", "effective_cost")
        u.add_column("commitment_discount_id", StringType(), doc="savings plan or reservation applied, if any")


def partition_by_team(table: Table) -> None:
    """Most dashboards filter by team. New files are split by team as well
    as by day; files already written keep the old layout."""
    with table.update_spec() as s:
        s.add_identity("team")


def snapshot_after_day(table: Table, day: dt.date) -> int:
    """The newest snapshot in main's history that loaded the given day."""
    snap = table.current_snapshot()
    while snap is not None:
        if snap.summary and snap.summary.additional_properties.get(DAY) == str(day) \
                and snap.summary.operation.value == "append":
            return snap.snapshot_id
        snap = table.snapshot_by_id(snap.parent_snapshot_id) if snap.parent_snapshot_id else None
    raise LookupError(f"no load of {day} in main's history")


def tag(table: Table, name: str, snapshot_id: int, max_age: dt.timedelta | None = None) -> None:
    """A named, immutable pointer to a snapshot, e.g. the numbers at month-end
    close. Tagged snapshots survive snapshot expiry until the tag expires."""
    ms = table.manage_snapshots()
    ms.create_tag(snapshot_id, name, max_ref_age_ms=int(max_age.total_seconds() * 1000) if max_age else None)
    ms.commit()


def rollback(table: Table, snapshot_id: int) -> None:
    """Point main back at an earlier snapshot. The later snapshots stay in
    the metadata (and their files on disk) until they expire."""
    table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()


def batch_ids(table: Table) -> list[str]:
    return [s.summary.additional_properties.get(BATCH_ID, "") for s in table.snapshots() if s.summary]
