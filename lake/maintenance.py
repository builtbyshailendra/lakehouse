"""Table maintenance: orphan files (more to come in step 4).

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
import time

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
            for e in m.fetch_manifest_entry(table.io, discard_deleted=False):
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
