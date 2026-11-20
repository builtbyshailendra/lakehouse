"""Iceberg table definitions.

Field IDs are set explicitly: Iceberg tracks columns by ID, not by name, which
is what makes renames and drops safe later (step 3).
"""

from __future__ import annotations

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DecimalType, DoubleType, NestedField, StringType, TimestampType

NAMESPACE = "billing"
COST = f"{NAMESPACE}.cost"
INCIDENTS = f"{NAMESPACE}.incidents"

COST_SCHEMA = Schema(
    NestedField(1, "line_id", StringType(), required=True),
    NestedField(2, "usage_start", TimestampType(), required=True),  # UTC, hour of usage
    NestedField(3, "account_id", StringType(), required=True),
    NestedField(4, "team", StringType(), required=True),
    NestedField(5, "service", StringType(), required=True),
    NestedField(6, "region", StringType(), required=True),
    NestedField(7, "resource_id", StringType(), required=True),
    NestedField(8, "usage_type", StringType(), required=True),
    NestedField(9, "usage_quantity", DoubleType(), required=True),
    NestedField(10, "unit", StringType(), required=True),
    # Money as decimals, never floats: sums must reconcile to the cent.
    NestedField(11, "list_cost", DecimalType(14, 6), required=True),
    NestedField(12, "billed_cost", DecimalType(14, 6), required=True),
    identifier_field_ids=[1],
)

# Hidden partitioning: the partition is derived from usage_start, so queries
# filter on the real column and Iceberg prunes; nobody has to know about a
# separate "dt" column.
COST_SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="usage_day"))

INCIDENT_SCHEMA = Schema(
    NestedField(1, "incident_id", StringType(), required=True),
    NestedField(2, "team", StringType(), required=True),
    NestedField(3, "service", StringType(), required=True),
    NestedField(4, "region", StringType(), required=True),
    NestedField(5, "severity", StringType(), required=True),
    NestedField(6, "started_at", TimestampType(), required=True),
    NestedField(7, "resolved_at", TimestampType(), required=True),
    identifier_field_ids=[1],
)

COST_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    # keep the metadata.json history bounded; old ones are deleted after commit
    "write.metadata.previous-versions-max": "50",
    "write.metadata.delete-after-commit.enabled": "true",
}


def create(cat: Catalog) -> tuple[Table, Table]:
    cat.create_namespace_if_not_exists(NAMESPACE)
    cost = cat.create_table_if_not_exists(COST, schema=COST_SCHEMA, partition_spec=COST_SPEC, properties=COST_PROPERTIES)
    inc = cat.create_table_if_not_exists(INCIDENTS, schema=INCIDENT_SCHEMA)
    return cost, inc
