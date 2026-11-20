"""Step 1: the generator, loading, reading back with two engines, pruning,
and the metadata tree read by hand."""

import datetime as dt
import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc

from lake import gen, ingest, meta
from lake.config import duck

D0 = gen.START


def test_generator_is_deterministic_and_well_formed():
    a, b = gen.day_batch(D0), gen.day_batch(D0)
    assert a.equals(b)
    assert a.num_rows == 77_600
    assert pc.count_distinct(a["line_id"]).as_py() == a.num_rows
    starts = a["usage_start"]
    assert pc.min(starts).as_py() == dt.datetime(2026, 8, 1, 0)
    assert pc.max(starts).as_py() == dt.datetime(2026, 8, 1, 23)
    assert a.schema.field("billed_cost").type == pa.decimal128(14, 6)
    # discounts only ever lower the bill
    assert pc.all(pc.less_equal(a["billed_cost"], a["list_cost"])).as_py()
    assert gen.day_batch(D0 + dt.timedelta(days=1)).num_rows == 77_600


def test_loaded_rows_read_back_exactly(cost):
    days = gen.days(3)
    expected = pa.concat_tables([ingest.conform(gen.day_batch(d), cost) for d in days])
    for d in days:
        ingest.load_day(cost, gen.day_batch(d), d)
    got = cost.scan().to_arrow().sort_by("line_id")
    assert got.equals(expected.sort_by("line_id"))


def _truth(days):
    total = Decimal(0)
    rows = 0
    for d in days:
        b = gen.day_batch(d)
        rows += b.num_rows
        total += pc.sum(b["billed_cost"]).as_py()
    return rows, total


def test_duckdb_and_pyiceberg_agree_on_every_snapshot(cost):
    """Two independent readers (PyIceberg, DuckDB's iceberg extension) and
    the generator all agree, for every snapshot in the history."""
    days = gen.days(4)
    for d in days:
        ingest.load_day(cost, gen.day_batch(d), d)
    c = duck()
    snaps = cost.snapshots()
    assert len(snaps) == 4
    for i, s in enumerate(snaps):
        rows, total = _truth(days[: i + 1])
        duck_rows, duck_total = c.execute(
            f"SELECT count(*), sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}', snapshot_from_id={s.snapshot_id})"
        ).fetchone()
        py = cost.scan(snapshot_id=s.snapshot_id, selected_fields=("billed_cost",)).to_arrow()
        assert (duck_rows, duck_total) == (rows, total)
        assert (py.num_rows, pc.sum(py["billed_cost"]).as_py()) == (rows, total)


def test_filter_on_the_real_column_prunes_partitions(cost):
    days = gen.days(5)
    for d in days:
        ingest.load_day(cost, gen.day_batch(d), d)
    assert len(list(cost.scan().plan_files())) == 5
    one = list(cost.scan(row_filter=ingest.day_filter(days[2])).plan_files())
    assert len(one) == 1 and "usage_day=2026-08-03" in one[0].file.file_path
    # an hour range inside one day: still one file
    hour = cost.scan(row_filter="usage_start >= '2026-08-04T10:00:00' AND usage_start < '2026-08-04T11:00:00'")
    assert len(list(hour.plan_files())) == 1
    rows = hour.to_arrow()
    # 3,200 hourly resources (the other 800 are storage, billed once a day at 00:00)
    assert rows.num_rows == 3_200
    assert all(t.hour == 10 for t in rows["usage_start"].to_pylist())


def test_metadata_tree_read_by_hand(cost):
    for d in gen.days(3):
        ingest.load_day(cost, gen.day_batch(d), d)
    t = meta.tree(cost.metadata_location)
    assert t.format_version == 2
    assert t.snapshots == 3
    assert t.current_snapshot_id == cost.current_snapshot().snapshot_id
    # one manifest per append (fast append), one data file per day
    assert len(t.manifests) == 3
    assert len(t.data_files) == 3
    assert t.records == 3 * 77_600
    assert all(os.path.exists(f["file_path"]) for f in t.data_files)
    # the stats in the manifests match what the table reports
    assert t.records == int(cost.current_snapshot().summary["total-records"])


def test_reloading_a_day_replaces_it(cost):
    days = gen.days(2)
    for d in days:
        ingest.load_day(cost, gen.day_batch(d), d)
    before = cost.scan().to_arrow().num_rows

    # the same export again: recorded in the history, so skipped
    assert ingest.load_day(cost, gen.day_batch(days[0]), days[0]).skipped

    # a restatement of day 0: every cost 10% lower, sent as a new batch
    restated = gen.restate(gen.day_batch(days[0]), 0.9)
    r = ingest.load_day(cost, restated, days[0], batch_id="cost-2026-08-01-v2")
    assert not r.skipped
    after = cost.scan().to_arrow()
    assert after.num_rows == before
    day0 = after.filter(pc.less(after["usage_start"], pa.scalar(dt.datetime(2026, 8, 2), pa.timestamp("us"))))
    assert pc.sum(day0["billed_cost"]).as_py() == pc.sum(restated["billed_cost"]).as_py()
