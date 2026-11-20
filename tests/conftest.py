import os
import uuid

import pytest
from sqlalchemy import text

from lake import tables
from lake.config import catalog

TEST_URI = os.environ.get("LAKE_TEST_PG_URI", "postgresql+psycopg2://postgres@127.0.0.1:5433/lakehouse_test")


def drop_catalog(cat):
    with cat.engine.begin() as c:
        for t in ("iceberg_tables", "iceberg_namespace_properties"):
            c.execute(text(f"DELETE FROM {t} WHERE catalog_name = :n"), {"n": cat.name})
    cat.engine.dispose()


@pytest.fixture
def cat(tmp_path):
    """A fresh catalog per test: its own name in the shared PostgreSQL
    catalog tables, its own warehouse directory."""
    c = catalog("t" + uuid.uuid4().hex[:12], uri=TEST_URI, warehouse=str(tmp_path / "wh"))
    yield c
    drop_catalog(c)


@pytest.fixture
def cost(cat):
    return tables.create(cat)[0]
