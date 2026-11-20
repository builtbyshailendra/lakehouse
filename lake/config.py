"""Where the catalog and the data live, and how to open them.

The catalog is PyIceberg's SQL catalog in the local PostgreSQL (the 5433
test cluster): one row per table pointing at its current metadata file.
The data and metadata files live in a warehouse directory on drive D.
"""

from __future__ import annotations

import os

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

PG_URI = os.environ.get("LAKE_PG_URI", "postgresql+psycopg2://postgres@127.0.0.1:5433/lakehouse")
# PyIceberg on Windows rejects file:///D:/..., but plain D:/... works for it
# and for DuckDB, so locations are written as plain paths.
WAREHOUSE = os.environ.get("LAKE_WAREHOUSE", "D:/cache/lakehouse-work/warehouse")
DUCKDB_EXTENSIONS = os.environ.get("LAKE_DUCKDB_EXT", "D:/cache/duckdb-ext")


def catalog(name: str = "lake", uri: str = PG_URI, warehouse: str = WAREHOUSE) -> SqlCatalog:
    warehouse = warehouse.replace("\\", "/")
    os.makedirs(warehouse, exist_ok=True)
    return SqlCatalog(name, uri=uri, warehouse=warehouse)


def duck() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that can read Iceberg tables and Avro files."""
    c = duckdb.connect()
    c.execute(f"SET extension_directory = '{DUCKDB_EXTENSIONS}'")
    c.execute("LOAD iceberg")
    c.execute("LOAD avro")
    return c
