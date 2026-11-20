"""Step 2: commits are atomic, idempotent and safe under concurrency.

Crashes are real process deaths (os._exit in a subprocess); races are real
processes against the same PostgreSQL catalog, plus deterministic two-writer
cases in-process.
"""

import datetime as dt
import json
import os
import random
import subprocess
import sys
import threading
from decimal import Decimal

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import ValidationException

from lake import gen, ingest, maintenance, tables
from lake.config import duck
from tests.conftest import TEST_URI

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
D0, D1, D2 = gen.days(3)


def run_worker(cat, days, crash=None, version="", factor=1.0, from_dir=None, wait=True):
    env = {k: v for k, v in os.environ.items() if k != "LAKE_CRASH"}
    if crash:
        env["LAKE_CRASH"] = crash
    args = [sys.executable, "-m", "lake.worker", "--catalog", cat.name, "--uri", TEST_URI,
            "--warehouse", cat.properties["warehouse"], "--days", ",".join(map(str, days))]
    if version:
        args += ["--version", version, "--factor", str(factor)]
    if from_dir:
        args += ["--from-dir", from_dir]
    p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.communicate(timeout=600) + (p.returncode,) if wait else p


def lines(out):
    return [json.loads(x) for x in out.splitlines() if x.startswith("{")]


def day_rows(table, day):
    t = table.scan(row_filter=ingest.day_filter(day), selected_fields=("billed_cost",)).to_arrow()
    return t.num_rows, pc.sum(t["billed_cost"]).as_py()


def billed(batch):
    return pc.sum(batch["billed_cost"]).as_py()


# --- crashes -----------------------------------------------------------------

@pytest.mark.parametrize("point", ["after-data-files", "after-metadata-json"])
def test_a_crash_before_the_pointer_swap_changes_nothing(cat, cost, point):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    before = cost.metadata_location

    out, err, code = run_worker(cat, [D1], crash=point)
    assert code == 137, err

    cost.refresh()
    assert cost.metadata_location == before
    assert len(cost.snapshots()) == 1
    assert cost.scan().to_arrow().num_rows == 77_600

    # What the dead writer left behind: files nothing refers to.
    orphans = maintenance.orphan_files(cost, older_than=dt.timedelta(0))
    assert any(f.endswith(".parquet") for f in orphans)
    assert any(os.path.basename(f).startswith("snap-") for f in orphans)  # its manifest list
    assert any(f.endswith(".metadata.json") for f in orphans) == (point == "after-metadata-json")

    # Rerun the load, then clean up: the table is exactly right.
    out, err, code = run_worker(cat, [D1])
    assert code == 0, err
    assert lines(out)[0]["skipped"] is False
    removed = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    assert sorted(removed) == orphans
    cost.refresh()
    assert day_rows(cost, D1) == (77_600, billed(gen.day_batch(D1)))
    assert duck().execute(f"SELECT count(*) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == 2 * 77_600


def test_a_crash_after_the_commit_then_a_rerun_is_a_no_op(cat, cost):
    # The commit landed but the writer never heard back: the classic
    # "did it work?" case. The rerun finds its batch id in the history.
    out, err, code = run_worker(cat, [D0], crash="after-commit")
    assert code == 137
    cost.refresh()
    assert day_rows(cost, D0)[0] == 77_600

    out, err, code = run_worker(cat, [D0])
    assert code == 0, err
    assert lines(out)[0]["skipped"] is True
    cost.refresh()
    assert len(cost.snapshots()) == 1
    assert day_rows(cost, D0)[0] == 77_600
    assert maintenance.orphan_files(cost, older_than=dt.timedelta(0)) == []


# --- two writers, deterministic ----------------------------------------------

def _set_isolation(table, level):
    with table.transaction() as tx:
        tx.set_properties({"write.update.isolation-level": level, "write.delete.isolation-level": level})


def test_serializable_refuses_the_second_of_two_racing_first_loads(cat, cost):
    a, b = cat.load_table(tables.COST), cat.load_table(tables.COST)  # both see an empty day
    v1, v2 = gen.restate(gen.day_batch(D0), 0.9), gen.restate(gen.day_batch(D0), 0.8)
    a.overwrite(ingest.conform(v1, a), overwrite_filter=ingest.day_filter(D0))
    with pytest.raises(ValidationException, match="Added data files were found matching the filter"):
        b.overwrite(ingest.conform(v2, b), overwrite_filter=ingest.day_filter(D0))
    cost.refresh()
    assert day_rows(cost, D0) == (77_600, billed(v1))
    # PyIceberg deletes the refused commit's manifests but not its Parquet
    # files: they are orphans now, waiting for the cleanup job.
    left = maintenance.orphan_files(cost, older_than=dt.timedelta(0))
    assert len(left) == 1 and left[0].endswith(".parquet")


def test_snapshot_isolation_lets_racing_first_loads_duplicate_a_day(cat, cost):
    """The reason the default is serializable: under snapshot isolation the
    second writer's delete matched nothing (the day was empty when it
    started), so its retry just appends, and the day is there twice."""
    _set_isolation(cost, "snapshot")
    a, b = cat.load_table(tables.COST), cat.load_table(tables.COST)
    v1, v2 = gen.restate(gen.day_batch(D0), 0.9), gen.restate(gen.day_batch(D0), 0.8)
    a.overwrite(ingest.conform(v1, a), overwrite_filter=ingest.day_filter(D0))
    b.overwrite(ingest.conform(v2, b), overwrite_filter=ingest.day_filter(D0))
    cost.refresh()
    assert day_rows(cost, D0) == (2 * 77_600, billed(v1) + billed(v2))


def test_load_day_recovers_from_a_lost_race(cat, cost):
    a, b = cat.load_table(tables.COST), cat.load_table(tables.COST)
    ingest.load_day(a, gen.restate(gen.day_batch(D0), 0.9), D0, batch_id="v1")
    r = ingest.load_day(b, gen.restate(gen.day_batch(D0), 0.8), D0, batch_id="v2")
    assert r.conflicts == 1 and not r.skipped
    cost.refresh()
    assert day_rows(cost, D0) == (77_600, billed(gen.restate(gen.day_batch(D0), 0.8)))


# --- many writers, real processes --------------------------------------------

@pytest.fixture(scope="module")
def exports(tmp_path_factory):
    d = tmp_path_factory.mktemp("exports")
    for day in gen.days(24):
        pq.write_table(gen.day_batch(day), d / f"{day}.parquet")
    return str(d)


def test_writers_on_different_days_all_land(cat, cost, exports):
    days = gen.days(24)
    procs = [run_worker(cat, days[i::6], from_dir=exports, wait=False) for i in range(6)]
    results = []
    for p in procs:
        out, err = p.communicate(timeout=600)
        assert p.returncode == 0, err
        results += lines(out)
    assert len(results) == 24 and not any(r["skipped"] for r in results)
    # they really did collide
    assert sum(r["pyiceberg_retries"] + r["conflicts"] for r in results) > 0
    cost.refresh()
    want = sum((billed(pq.read_table(f"{exports}/{d}.parquet")) for d in days), Decimal(0))
    got = duck().execute(f"SELECT count(*), sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()
    assert got == (24 * 77_600, want)
    assert len(cost.snapshots()) == 24


def test_writers_racing_on_the_same_days_leave_one_version_each(cat, cost, exports):
    days = gen.days(3)
    versions = {f"w{i}": round(1 - i * 0.01, 2) for i in range(6)}
    procs = []
    for i, (v, f) in enumerate(versions.items()):
        order = days[:]
        random.Random(i).shuffle(order)
        procs.append(run_worker(cat, order, version=v, factor=f, from_dir=exports, wait=False))
    results = []
    for p in procs:
        out, err = p.communicate(timeout=600)
        assert p.returncode == 0, err
        results += lines(out)
    assert len(results) == 18
    assert sum(r["conflicts"] for r in results) > 0, "the writers never collided; the test proved nothing"
    cost.refresh()
    for d in days:
        rows, total = day_rows(cost, d)
        assert rows == 77_600
        base = gen.day_batch(d)
        assert total in {billed(gen.restate(base, f)) for f in versions.values()}


# --- orphan cleanup ----------------------------------------------------------

def _paused_writer(monkeypatch, cat):
    """Start a load of D1 in a thread and hold it just before the catalog
    commit: its data files are written but nothing refers to them yet."""
    entered, release = threading.Event(), threading.Event()
    commit = SqlCatalog.commit_table

    def held(self, table, requirements, updates):
        entered.set()
        release.wait(60)
        return commit(self, table, requirements, updates)

    monkeypatch.setattr(SqlCatalog, "commit_table", held)
    writer = cat.load_table(tables.COST)
    errors = []

    def run():
        try:
            writer.overwrite(ingest.conform(gen.day_batch(D1), writer), overwrite_filter=ingest.day_filter(D1))
        except Exception as e:  # pragma: no cover - reported below
            errors.append(e)

    th = threading.Thread(target=run)
    th.start()
    assert entered.wait(60)
    return th, release, errors


def test_orphan_cleanup_with_no_age_limit_breaks_a_running_write(cat, cost, monkeypatch):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    th, release, errors = _paused_writer(monkeypatch, cat)

    removed = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    assert any(f.endswith(".parquet") for f in removed)  # the running writer's data

    release.set()
    th.join()
    assert errors == []  # the commit itself succeeds...
    cost.refresh()
    assert len(cost.snapshots()) == 2
    with pytest.raises(OSError):  # ...and the table now points at a deleted file
        cost.scan().to_arrow()


def test_orphan_cleanup_with_an_age_limit_spares_a_running_write(cat, cost, monkeypatch):
    ingest.load_day(cost, gen.day_batch(D0), D0)
    th, release, errors = _paused_writer(monkeypatch, cat)

    assert maintenance.remove_orphan_files(cost, older_than=dt.timedelta(hours=1)) == []

    release.set()
    th.join()
    assert errors == []
    cost.refresh()
    assert cost.scan().to_arrow().num_rows == 2 * 77_600
