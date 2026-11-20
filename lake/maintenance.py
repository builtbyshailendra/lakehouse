"""Table maintenance: compaction, snapshot expiry, orphan files.

Compaction rewrites many small files as few big ones. It is a *file-level*
replace: "remove exactly these files, add these", not "replace whatever
matches this filter". So a file appended meanwhile is never touched, and if
one of the files being rewritten was replaced meanwhile (a restatement), the
commit is refused instead of resurrecting the old rows.

Expiry removes old snapshots from the metadata (tagged ones and branch heads
are kept). PyIceberg 0.12 doesn't delete any files when it expires; the
files only the expired snapshots used become orphans, for the cleanup below.

An orphan is a file under the table's location that no metadata refers to:
the leftovers of a writer that died mid-commit (everything it wrote), or of
a commit that was refused (PyIceberg 0.12 deletes the manifests of a refused
commit but not its Parquet files).

The trap: a writer that is *still running* has written files that nothing
refers to yet either. Delete "orphans" with no age limit and you delete its
data; when it commits, the table points at files that no longer exist. That
is why Iceberg's Spark procedure defaults to only deleting files older than
3 days.
"""

from __future__ import annotations

import datetime as dt
import os
import random
import time
from dataclasses import dataclass

from pyiceberg.exceptions import CommitFailedException, ValidationException
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files
from pyiceberg.table import Table


def _norm(path: str) -> str:
    if path.startswith("file://"):
        path = path[len("file://"):]
    return os.path.normcase(os.path.normpath(path))


def referenced_files(table: Table) -> set[str]:
    """Every file any snapshot or metadata version still points at."""
    md = table.metadata
    refs = {table.metadata_location}
    refs.update(e.metadata_file for e in md.metadata_log)
    refs.update(s.statistics_path for s in md.statistics)
    for snap in md.snapshots:
        refs.add(snap.manifest_list)
        for m in snap.manifests(table.io):
            refs.add(m.manifest_path)
            # Only live entries. A DELETED entry names a file this snapshot
            # removed: if an older snapshot still uses the file, that snapshot's
            # manifests list it as live; if not, nothing can read it any more.
            for e in m.fetch_manifest_entry(table.io, discard_deleted=True):
                refs.add(e.data_file.file_path)
    return {_norm(p) for p in refs}


def all_files(table: Table) -> list[str]:
    out = []
    for root, _, files in os.walk(table.location()):
        out.extend(os.path.join(root, f) for f in files)
    return out


def orphan_files(table: Table, older_than: dt.timedelta) -> list[str]:
    table.refresh()
    refs = referenced_files(table)
    cutoff = time.time() - older_than.total_seconds()
    return sorted(f for f in all_files(table) if _norm(f) not in refs and os.path.getmtime(f) < cutoff)


def remove_orphan_files(table: Table, older_than: dt.timedelta = dt.timedelta(days=3), dry_run: bool = False) -> list[str]:
    found = orphan_files(table, older_than)
    if not dry_run:
        for f in found:
            os.remove(f)
    return found


@dataclass
class Compaction:
    files_before: int
    files_after: int
    rows: int
    bytes_written: int
    attempts: int


def compact(table: Table, row_filter: BooleanExpression | str, sort_by: list[tuple[str, str]] | None = None,
            attempts: int = 10) -> Compaction | None:
    """Rewrite the files that match a filter as few target-sized files,
    optionally sorted (clustering rows so Parquet min/max stats can skip
    row groups). Files are read whole: rows outside the filter that share a
    file are rewritten too, never dropped. Retries on conflict by planning
    again from the new table state."""
    for attempt in range(1, attempts + 1):
        table.refresh()
        tasks = list(table.scan(row_filter=row_filter).plan_files())
        if any(t.delete_files for t in tasks):
            raise NotImplementedError("files with delete files attached")
        if not tasks:
            return None
        df = ArrowScan(table.metadata, table.io, table.schema(), AlwaysTrue()).to_table(tasks)
        if sort_by:
            df = df.sort_by(sort_by)
        try:
            with table.transaction() as tx:
                new = list(_dataframe_to_data_files(tx.table_metadata, df, table.io))
                with tx.update_snapshot(snapshot_properties={"lake.op": "compact"}).overwrite() as ow:
                    for t in tasks:
                        ow.delete_data_file(t.file)
                    for f in new:
                        ow.append_data_file(f)
            return Compaction(len(tasks), len(new), df.num_rows, sum(f.file_size_in_bytes for f in new), attempt)
        except (CommitFailedException, ValidationException):
            if attempt == attempts:
                raise
            time.sleep(min(0.05 * 2**attempt, 2.0) * random.uniform(0.5, 1.5))
    return None


def expire_snapshots(table: Table, older_than: dt.datetime) -> int:
    """Drop snapshots older than a moment from the metadata, except branch
    heads and tagged snapshots. Returns how many were removed."""
    before = len(table.snapshots())
    table.maintenance.expire_snapshots().older_than(older_than).commit()
    table.refresh()
    return before - len(table.snapshots())
