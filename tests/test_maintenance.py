"""Step 4: small files and compaction, row-level deletes, snapshot expiry."""

import datetime as dt
import re
import threading

import pyarrow.compute as pc
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from lake import evolution, gen, ingest, maintenance, tables
from lake.config import duck

D0, D1, D2 = gen.days(3)


def hours(table, day, batch=None):
    b = ingest.conform(batch if batch is not None else gen.day_batch(day), table)
    return [b.filter(pc.equal(pc.hour(b["usage_start"]), h)) for h in range(24)]


def n_files(table):
    return len(list(table.scan().plan_files()))


def total(table, snapshot_id=None):
    return pc.sum(table.scan(snapshot_id=snapshot_id, selected_fields=("billed_cost",)).to_arrow()["billed_cost"]).as_py()


def billed(batch):
    return pc.sum(batch["billed_cost"]).as_py()


def test_compaction_turns_hourly_files_into_one_and_keeps_every_row(cost):
    for part in hours(cost, D0):
        cost.append(part)
    assert n_files(cost) == 24 and len(cost.current_snapshot().manifests(cost.io)) == 24
    before = cost.scan().to_arrow().sort_by("line_id")

    r = maintenance.compact(cost, ingest.day_filter(D0))
    assert (r.files_before, r.files_after, r.rows, r.attempts) == (24, 1, 77_600, 1)
    assert cost.scan().to_arrow().sort_by("line_id").equals(before)
    assert cost.current_snapshot().summary.operation.value == "overwrite"
    assert duck().execute(f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == billed(before)


def _hold_first_commit(monkeypatch, thread_name):
    """Hold the named thread's first catalog commit until released."""
    entered, release = threading.Event(), threading.Event()
    commit = SqlCatalog.commit_table
    held = []

    def maybe_hold(self, table, requirements, updates):
        if threading.current_thread().name == thread_name and not held:
            held.append(1)
            entered.set()
            release.wait(60)
        return commit(self, table, requirements, updates)

    monkeypatch.setattr(SqlCatalog, "commit_table", maybe_hold)
    return entered, release


def _compact_in_thread(cat, day):
    out = {}

    def run():
        out["result"] = maintenance.compact(cat.load_table(tables.COST), ingest.day_filter(day))

    th = threading.Thread(target=run, name="compactor")
    th.start()
    return th, out


def test_compaction_racing_an_append_keeps_the_append(cat, cost, monkeypatch):
    parts = hours(cost, D0)
    for part in parts[:23]:
        cost.append(part)
    entered, release = _hold_first_commit(monkeypatch, "compactor")
    th, out = _compact_in_thread(cat, D0)
    assert entered.wait(60)
    cost.append(parts[23])  # the last hour lands while the compactor commits
    release.set()
    th.join()

    # PyIceberg refused the first attempt (under serializable isolation any
    # concurrent append counts); the retry planned again and took all 24.
    assert out["result"].attempts == 2 and out["result"].files_before == 24
    cost.refresh()
    assert cost.scan().to_arrow().num_rows == 77_600 and n_files(cost) == 1
    assert total(cost) == billed(gen.day_batch(D0))


@pytest.mark.parametrize("isolation", ["serializable", "snapshot"])
def test_compaction_racing_a_restatement_never_brings_old_rows_back(cat, cost, monkeypatch, isolation):
    with cost.transaction() as tx:
        tx.set_properties({"write.update.isolation-level": isolation, "write.delete.isolation-level": isolation})
    for part in hours(cost, D0):
        cost.append(part)
    entered, release = _hold_first_commit(monkeypatch, "compactor")
    th, out = _compact_in_thread(cat, D0)
    assert entered.wait(60)
    restated = gen.restate(gen.day_batch(D0), 0.9)
    ingest.load_day(cost, restated, D0, batch_id="v2")  # replaces all 24 files meanwhile
    release.set()
    th.join()

    # The 24 files the compactor read no longer exist in the table, so its
    # commit is refused whatever the isolation level; it plans again.
    assert out["result"].attempts == 2 and out["result"].files_before == 1
    cost.refresh()
    assert total(cost) == billed(restated)


def _row_groups(c, sql):
    plan = c.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    scanned, of = re.search(r"Row Groups Scanned: (\d+) / (\d+)", plan).groups()
    return int(scanned), int(of)


def test_sorted_compaction_lets_readers_skip_row_groups(cost):
    with cost.transaction() as tx:
        tx.set_properties({"write.parquet.row-group-limit": "10000"})
    ingest.load_day(cost, gen.day_batch(D0), D0)
    c = duck()
    q = f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}') WHERE team = 'payments'"
    assert _row_groups(c, q) == (8, 8)  # rows in time order: every row group has every team
    want = c.execute(q).fetchone()[0]

    maintenance.compact(cost, ingest.day_filter(D0), sort_by=[("team", "ascending"), ("usage_start", "ascending")])
    q = f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}') WHERE team = 'payments'"
    scanned, of = _row_groups(c, q)
    assert of == 8 and scanned <= 2
    assert c.execute(q).fetchone()[0] == want


def test_a_row_level_delete_rewrites_every_file_it_touches(cost):
    for d in (D0, D1, D2):
        ingest.load_day(cost, gen.day_batch(d), d)
    before = cost.current_snapshot().snapshot_id
    gone = sum(pc.sum(pc.equal(gen.day_batch(d)["account_id"], "growth-staging")).as_py() for d in (D0, D1, D2))

    cost.delete("account_id = 'growth-staging'")  # copy-on-write: PyIceberg 0.12 has no merge-on-read deletes
    s = cost.current_snapshot().summary
    assert int(s["deleted-data-files"]) == 3 and int(s["added-data-files"]) == 3
    assert int(s["added-records"]) == 3 * 77_600 - gone  # rewrote every other row to drop these
    assert cost.scan().to_arrow().num_rows == 3 * 77_600 - gone
    assert cost.scan(snapshot_id=before).to_arrow().num_rows == 3 * 77_600  # still there in history


def test_a_partition_aligned_delete_rewrites_nothing(cost):
    for d in (D0, D1):
        ingest.load_day(cost, gen.day_batch(d), d)
    cost.delete(ingest.day_filter(D0))
    s = cost.current_snapshot().summary
    assert int(s["deleted-data-files"]) == 1 and int(s.get("added-data-files") or 0) == 0
    assert cost.scan().to_arrow().num_rows == 77_600


def test_expiry_keeps_tags_and_head_and_cleanup_reclaims_the_rest(cost):
    ingest.load_day(cost, gen.day_batch(D0), D0, batch_id="v1")
    evolution.tag(cost, "close", cost.current_snapshot().snapshot_id)
    for i, f in enumerate((0.97, 0.95, 0.9), start=2):
        ingest.load_day(cost, gen.restate(gen.day_batch(D0), f), D0, batch_id=f"v{i}")
    old = [s.snapshot_id for s in cost.snapshots()]
    assert len(old) == 7  # v1 append, then a delete + append per restatement
    data_on_disk = [f for f in maintenance.all_files(cost) if f.endswith(".parquet")]
    assert len(data_on_disk) == 4

    removed = maintenance.expire_snapshots(cost, dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1))
    assert removed == 5
    assert {s.snapshot_id for s in cost.snapshots()} == {cost.refs()["close"].snapshot_id, cost.current_snapshot().snapshot_id}
    with pytest.raises(Exception):
        cost.scan(snapshot_id=old[2]).to_arrow()

    orphans = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    assert sum(f.endswith(".parquet") for f in orphans) == 2  # v2 and v3 data; v1 (tagged) and v4 (head) stay
    assert total(cost, cost.refs()["close"].snapshot_id) == billed(gen.day_batch(D0))
    assert total(cost) == billed(gen.restate(gen.day_batch(D0), 0.9))
    assert duck().execute(f"SELECT count(*) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == 77_600


def dead_manifests(table):
    """Manifests in the current snapshot that hold only deleted entries."""
    return sum(1 for m in table.current_snapshot().manifests(table.io) if not m.has_added_files() and not m.has_existing_files())


def test_compaction_is_invisible_to_incremental_readers(cost):
    """Compaction commits an 'overwrite' (Iceberg's replace): a consumer
    reading new appends since some snapshot doesn't see compacted rows again.
    Done as delete + append instead, it would."""
    b = ingest.conform(gen.day_batch(D0), cost)
    for q in range(4):
        cost.append(b.filter(pc.equal(pc.divide(pc.hour(b["usage_start"]), 6), q)))
    since = cost.current_snapshot().snapshot_id
    maintenance.compact(cost, ingest.day_filter(D0))
    ingest.load_day(cost, gen.day_batch(D1), D1)
    assert cost.incremental_append_scan(from_snapshot_id_exclusive=since).to_arrow().num_rows == 77_600


def test_compaction_leaves_dead_manifests_until_the_next_append(cost):
    """PyIceberg 0.12's file-level overwrite carries forward manifests that
    hold only deleted entries; readers still open them. The next append
    (a fast append) drops them."""
    for d in (D0, D1):
        b = ingest.conform(gen.day_batch(d), cost)
        for q in range(4):
            cost.append(b.filter(pc.equal(pc.divide(pc.hour(b["usage_start"]), 6), q)))
    for d in (D0, D1):
        maintenance.compact(cost, ingest.day_filter(d))
    assert dead_manifests(cost) >= 1
    ingest.load_day(cost, gen.day_batch(D2), D2)
    assert dead_manifests(cost) == 0
    assert cost.scan().to_arrow().num_rows == 3 * 77_600


def test_files_named_only_by_deleted_entries_are_cleaned_up(cost):
    """After compacting two days, the first day's dead manifest is still in
    the current snapshot, naming its 4 old files as DELETED. Once the older
    snapshots expire nothing can read those files, so cleanup must remove
    them (the first version counted deleted entries as references and kept
    248 such files on the main table)."""
    for d in (D0, D1):
        b = ingest.conform(gen.day_batch(d), cost)
        for q in range(4):
            cost.append(b.filter(pc.equal(pc.divide(pc.hour(b["usage_start"]), 6), q)))
    for d in (D0, D1):
        maintenance.compact(cost, ingest.day_filter(d))
    assert dead_manifests(cost) >= 1
    maintenance.expire_snapshots(cost, dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1))
    removed = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    assert sum(f.endswith(".parquet") for f in removed) == 8
    assert sum(f.endswith(".parquet") for f in maintenance.all_files(cost)) == 2
    assert cost.scan().to_arrow().num_rows == 2 * 77_600
    assert duck().execute(f"SELECT count(*) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == 2 * 77_600
