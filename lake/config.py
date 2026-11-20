"""Where the catalog and the data live, and how to open them.

The catalog is PyIceberg's SQL catalog in PostgreSQL (by default the local
test cluster on port 5433): one row per table pointing at its current
metadata file. The data and metadata files live in a warehouse directory:
on drive D on the laptop this was built on, in the temp directory elsewhere.
"""

from __future__ import annotations

import os
import tempfile

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

PG_URI = os.environ.get("LAKE_PG_URI", "postgresql+psycopg2://postgres@127.0.0.1:5433/lakehouse")
_ON_D = os.name == "nt" and os.path.isdir("D:/")
WORK = os.environ.get("LAKE_WORK", "D:/cache/lakehouse-work" if _ON_D else os.path.join(tempfile.gettempdir(), "lakehouse-work"))
# PyIceberg on Windows rejects file:///D:/..., but plain D:/... works for it
# and for DuckDB, so locations are written as plain paths.
WAREHOUSE = os.environ.get("LAKE_WAREHOUSE", f"{WORK}/warehouse").replace("\\", "/")
# None: DuckDB's default (~/.duckdb/extensions)
DUCKDB_EXTENSIONS = os.environ.get("LAKE_DUCKDB_EXT", "D:/cache/duckdb-ext" if _ON_D else None)


def catalog(name: str = "lake", uri: str = PG_URI, warehouse: str = WAREHOUSE) -> SqlCatalog:
    warehouse = warehouse.replace("\\", "/")
    os.makedirs(warehouse, exist_ok=True)
    return SqlCatalog(name, uri=uri, warehouse=warehouse)


def duck(extensions: tuple[str, ...] = ("iceberg", "avro")) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that can read Iceberg tables and Avro files.
    Extensions are installed from DuckDB's repository the first time."""
    c = duckdb.connect()
    if DUCKDB_EXTENSIONS:
        c.execute(f"SET extension_directory = '{DUCKDB_EXTENSIONS}'")
    for ext in extensions:
        try:
            c.execute(f"LOAD {ext}")
        except duckdb.Error:
            c.execute(f"INSTALL {ext}")
            c.execute(f"LOAD {ext}")
    return c
