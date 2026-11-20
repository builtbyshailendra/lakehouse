"""Reading an Iceberg table's metadata by hand.

PyIceberg already knows how to read all of this. The point of reading the raw
files here (JSON with the standard library, Avro with DuckDB) is to see the
format itself:

    catalog row (PostgreSQL)  ->  metadata.json  (schemas, specs, snapshots, refs)
                                    -> manifest list  (Avro, one per snapshot)
                                       -> manifests   (Avro, each lists data files + stats)
                                          -> data files (Parquet)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .config import duck


@dataclass
class Tree:
    metadata_location: str
    metadata_bytes: int
    format_version: int
    snapshots: int
    current_snapshot_id: int | None
    manifest_list: str | None
    manifests: list[dict] = field(default_factory=list)
    data_files: list[dict] = field(default_factory=list)

    @property
    def records(self) -> int:
        return sum(f["record_count"] for f in self.data_files)

    @property
    def data_bytes(self) -> int:
        return sum(f["file_size_in_bytes"] for f in self.data_files)


def tree(metadata_location: str, snapshot_id: int | None = None) -> Tree:
    with open(metadata_location, encoding="utf-8") as f:
        meta = json.load(f)
    snap_id = snapshot_id if snapshot_id is not None else meta.get("current-snapshot-id")
    snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == snap_id), None)
    t = Tree(
        metadata_location=metadata_location,
        metadata_bytes=os.path.getsize(metadata_location),
        format_version=meta["format-version"],
        snapshots=len(meta.get("snapshots", [])),
        current_snapshot_id=snap_id,
        manifest_list=snap["manifest-list"] if snap else None,
    )
    if not snap:
        return t
    c = duck()
    t.manifests = [
        dict(zip(["manifest_path", "manifest_length", "added_files_count", "existing_files_count", "deleted_files_count", "added_snapshot_id"], r))
        for r in c.execute(
            "SELECT manifest_path, manifest_length, added_files_count, existing_files_count, deleted_files_count, added_snapshot_id "
            "FROM read_avro(?)", [snap["manifest-list"]]
        ).fetchall()
    ]
    for m in t.manifests:
        # status: 0 existing, 1 added, 2 deleted. Deleted entries are history,
        # not part of this snapshot.
        for status, path, records, size in c.execute(
            "SELECT status, data_file.file_path, data_file.record_count, data_file.file_size_in_bytes "
            "FROM read_avro(?)", [m["manifest_path"]]
        ).fetchall():
            if status != 2:
                t.data_files.append({"file_path": path, "record_count": records, "file_size_in_bytes": size, "manifest": m["manifest_path"]})
    return t


def describe(t: Tree) -> str:
    lines = [
        f"metadata.json   {os.path.basename(t.metadata_location)}  {t.metadata_bytes:,} bytes, format v{t.format_version}, {t.snapshots} snapshots",
        f"snapshot        {t.current_snapshot_id}",
        f"manifest list   {os.path.basename(t.manifest_list or '-')}  {len(t.manifests)} manifests",
    ]
    for m in t.manifests[:3]:
        lines.append(f"  manifest      {os.path.basename(m['manifest_path'])}  {m['manifest_length']:,} bytes, "
                     f"added {m['added_files_count']}, existing {m['existing_files_count']}, deleted {m['deleted_files_count']}")
    if len(t.manifests) > 3:
        lines.append(f"  ... {len(t.manifests) - 3} more manifests")
    lines.append(f"data files      {len(t.data_files)} Parquet files, {t.records:,} rows, {t.data_bytes:,} bytes")
    for f in t.data_files[:2]:
        lines.append(f"  {f['file_path'].split('/data/', 1)[-1]}  {f['record_count']:,} rows, {f['file_size_in_bytes']:,} bytes")
    return "\n".join(lines)
