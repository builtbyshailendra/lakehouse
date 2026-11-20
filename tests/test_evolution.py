"""Step 3: schema evolution, time travel, tags, rollback, partition
evolution. Checked with PyIceberg and DuckDB, and against reading the
Parquet files by column name (what a table without IDs would do)."""

import datetime as dt
import os

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.types import DecimalType, LongType, StringType

from lake import evolution, gen, ingest
from lake.config import duck

D0, D1, D2, D3 = gen.days(4)


def files_with_mtimes(table):
    return {f.file.file_path: os.path.getmtime(f.file.file_path) for f in table.scan().plan_files()}


def billed(batch):
    return pc.sum(batch["billed_cost"]).as_py()


def test_rename_is_metadata_only_and_old_files_read_by_id(cost):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    before = files_with_mtimes(cost)
    evolution.adopt_focus_names(cost)
    assert files_with_mtimes(cost) == before  # nothing rewritten
    ingest.load_day(cost, gen.day_batch(D1), D1)  # export still says billed_cost

    want = billed(gen.day_batch(D0)) + billed(gen.day_batch(D1))
    t = cost.scan().to_arrow()
    assert "billed_cost" not in t.column_names
    assert pc.sum(t["effective_cost"]).as_py() == want
    assert t["commitment_discount_id"].null_count == t.num_rows
    c = duck()
    assert c.execute(f"SELECT sum(effective_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == want

    # The same files read by column name: the old file has no effective_cost.
    files = [f.file.file_path for f in cost.scan().plan_files()]
    by_name = c.execute(f"SELECT count(effective_cost), count(billed_cost) FROM read_parquet({files!r}, union_by_name=true)").fetchone()
    assert by_name == (77_600, 77_600)  # half the costs are under each name


def test_a_dropped_then_re_added_column_does_not_bring_old_values_back(cost):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    with cost.update_schema() as u:
        u.delete_column("usage_type")
    with cost.update_schema() as u:
        u.add_column("usage_type", StringType())  # same name, new field ID
    b = gen.day_batch(D1)
    b = b.set_column(b.schema.get_field_index("usage_type"), "usage_type", pa.array(["v2"] * b.num_rows))
    ingest.load_day(cost, b, D1)

    t = cost.scan().to_arrow()
    assert pc.value_counts(t["usage_type"]).to_pylist() == [{"values": "v2", "counts": 77_600}, {"values": None, "counts": 77_600}]
    c = duck()
    assert c.execute(f"SELECT count(usage_type) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == 77_600
    # By name, the old file's dropped values come back to life.
    files = [f.file.file_path for f in cost.scan().plan_files()]
    assert c.execute(f"SELECT count(usage_type) FROM read_parquet({files!r}, union_by_name=true)").fetchone()[0] == 2 * 77_600


def test_safe_changes_are_allowed_and_unsafe_ones_refused(cost):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    with cost.update_schema() as u:
        u.update_column("list_cost", DecimalType(18, 6))  # widening precision is safe
    assert str(cost.schema().find_field("list_cost").field_type) == "decimal(18, 6)"
    assert cost.scan().to_arrow().num_rows == 77_600

    for change, message in [
        (lambda u: u.update_column("team", LongType()), "Cannot change column type"),
        (lambda u: u.update_column("list_cost", DecimalType(12, 6)), "Cannot change column type"),
        (lambda u: u.add_column("owner", StringType(), required=True), "cannot add required column"),
    ]:
        with pytest.raises(Exception, match=message):
            with cost.update_schema() as u:
                change(u)


def test_time_travel_shows_the_rows_and_the_schema_of_the_time(cost):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    old = cost.current_snapshot().snapshot_id
    evolution.adopt_focus_names(cost)
    ingest.load_day(cost, gen.day_batch(D1), D1)

    t = cost.scan(snapshot_id=old).to_arrow()
    assert t.num_rows == 77_600 and "billed_cost" in t.column_names and "effective_cost" not in t.column_names
    cols = [r[0] for r in duck().execute(
        f"DESCRIBE SELECT * FROM iceberg_scan('{cost.metadata_location}', snapshot_from_id={old})").fetchall()]
    assert "billed_cost" in cols and "commitment_discount_id" not in cols

    # and by timestamp: the snapshot current at a moment in the past
    ts = cost.snapshot_by_id(old).timestamp_ms
    assert cost.snapshot_as_of_timestamp(ts).snapshot_id == old


def test_a_tag_keeps_the_month_end_numbers_after_restatements(cost):
    for d in (D0, D1):
        ingest.load_day(cost, gen.day_batch(d), d)
    evolution.tag(cost, "close-2026-08", evolution.snapshot_after_day(cost, D1))
    closed = billed(gen.day_batch(D0)) + billed(gen.day_batch(D1))

    # a 3% credit arrives for D0 after the close
    ingest.load_day(cost, gen.restate(gen.day_batch(D0), 0.97), D0, batch_id="cost-2026-08-01-credit")
    now = pc.sum(cost.scan().to_arrow()["billed_cost"]).as_py()
    assert now < closed

    tagged = cost.refs()["close-2026-08"].snapshot_id
    assert pc.sum(cost.scan(snapshot_id=tagged).to_arrow()["billed_cost"]).as_py() == closed
    assert duck().execute(
        f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}', snapshot_from_id={tagged})").fetchone()[0] == closed


def test_rollback_undoes_a_bad_load_and_lets_the_fix_in(cost):
    for d in (D0, D1):
        ingest.load_day(cost, gen.day_batch(d), d)
    good = cost.current_snapshot().snapshot_id
    ingest.load_day(cost, gen.restate(gen.day_batch(D1), 100), D1, batch_id="cost-2026-08-02-v2")  # a x100 bug
    assert pc.sum(cost.scan().to_arrow()["billed_cost"]).as_py() > 50 * billed(gen.day_batch(D1))

    evolution.rollback(cost, good)
    assert cost.current_snapshot().snapshot_id == good
    assert pc.sum(cost.scan().to_arrow()["billed_cost"]).as_py() == billed(gen.day_batch(D0)) + billed(gen.day_batch(D1))
    # the bad batch is no longer in main's history, so a corrected v2 loads
    r = ingest.load_day(cost, gen.restate(gen.day_batch(D1), 0.99), D1, batch_id="cost-2026-08-02-v2")
    assert not r.skipped


def test_partition_evolution_prunes_new_data_and_reads_both_layouts(cost):
    for d in (D0, D1):
        ingest.load_day(cost, gen.day_batch(d), d)
    evolution.partition_by_team(cost)
    for d in (D2, D3):
        ingest.load_day(cost, gen.day_batch(d), d)

    specs = {f.file.spec_id for f in cost.scan().plan_files()}
    assert specs == {0, 1}
    team = "team = 'payments'"
    planned = list(cost.scan(row_filter=team).plan_files())
    # old layout: 1 file per day, can't prune by team; new layout: 1 of 8
    assert len(planned) == 4
    assert sum(1 for f in planned if f.file.spec_id == 1) == 2
    assert len(list(cost.scan().plan_files())) == 2 + 2 * 8

    want = sum(
        (pc.sum(pc.filter(gen.day_batch(d)["billed_cost"], pc.equal(gen.day_batch(d)["team"], "payments"))).as_py()
         for d in (D0, D1, D2, D3)),
    )
    assert pc.sum(cost.scan(row_filter=team).to_arrow()["billed_cost"]).as_py() == want
    assert duck().execute(
        f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}') WHERE {team}").fetchone()[0] == want

    # reloading an old day writes it in the new layout
    ingest.load_day(cost, gen.day_batch(D0), D0, batch_id="cost-2026-08-01-relayout")
    assert len([f for f in cost.scan(row_filter=ingest.day_filter(D0)).plan_files() if f.file.spec_id == 1]) == 8
