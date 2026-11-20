"""Crash points for the commit path, switched on by LAKE_CRASH.

An Iceberg commit happens in three stages, and a process can die between any
two of them:

    1. write the Parquet data files, the manifests and the manifest list
    2. write the new metadata.json
    3. swap the catalog's pointer to it: in the SQL catalog,
       UPDATE iceberg_tables SET metadata_location = new
        WHERE ... AND metadata_location = old        (compare-and-swap)

    LAKE_CRASH=after-data-files     die between 1 and 2
    LAKE_CRASH=after-metadata-json  die between 2 and 3
    LAKE_CRASH=after-commit         die after 3, before the caller hears back

The process exits with os._exit(137), like kill -9: no cleanup, no finally.
"""

from __future__ import annotations

import os

from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.catalog.sql import SqlCatalog

POINTS = ("after-data-files", "after-metadata-json", "after-commit")


def install(point: str | None = None) -> None:
    point = point or os.environ.get("LAKE_CRASH")
    if not point:
        return
    if point not in POINTS:
        raise ValueError(f"unknown crash point {point!r}; expected one of {POINTS}")

    commit = SqlCatalog.commit_table
    write_metadata = MetastoreCatalog._write_metadata

    def commit_table(self, table, requirements, updates):
        if point == "after-data-files":
            os._exit(137)
        resp = commit(self, table, requirements, updates)
        if point == "after-commit":
            os._exit(137)
        return resp

    def _write_metadata(metadata, io, metadata_path):
        write_metadata(metadata, io, metadata_path)
        if point == "after-metadata-json":
            os._exit(137)

    SqlCatalog.commit_table = commit_table
    MetastoreCatalog._write_metadata = staticmethod(_write_metadata)
