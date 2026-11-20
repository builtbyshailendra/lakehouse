"""Step 2 measurements.

1. Writers: 1, 2, 4, 8 processes load 32 days between them (different days,
   so nothing conflicts logically; they only compete for the catalog
   pointer). How do throughput and retries change?
2. Kill loop: 30 runs of a loader killed at a random moment (TerminateProcess,
   like kill -9), each restating 3 random days. After every kill each day
   must hold one complete version, never a mix or a partial; at the end the
   orphans are cleaned and DuckDB must agree.
"""

import datetime as dt
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from decimal import Decimal

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from lake import gen, ingest, maintenance, tables  # noqa: E402
from lake.config import WORK, catalog, duck  # noqa: E402
from tests.conftest import TEST_URI, drop_catalog  # noqa: E402



def fresh():
    name = "m" + uuid.uuid4().hex[:10]
    wh = f"{WORK}/{name}"
    cat = catalog(name, uri=TEST_URI, warehouse=wh)
    return cat, tables.create(cat)[0], wh


def worker(cat, wh, days, exports, version="", factor=1.0):
    args = [sys.executable, "-m", "lake.worker", "--catalog", cat.name, "--uri", TEST_URI, "--warehouse", wh,
            "--days", ",".join(map(str, days)), "--from-dir", exports]
    if version:
        args += ["--version", version, "--factor", str(factor)]
    return subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def writers(exports, reps=3):
    days = gen.days(32)
    print(f"writers  commits/s median (range of {reps} runs)  PyIceberg retries  load_day retries  orphan files   (per run, median)")
    for n in (1, 2, 4, 8):
        runs = []
        for _ in range(reps):
            cat, cost, wh = fresh()
            t = time.perf_counter()
            procs = [worker(cat, wh, days[i::n], exports) for i in range(n)]
            res = []
            for p in procs:
                out, err = p.communicate()
                assert p.returncode == 0, err
                res += [json.loads(x) for x in out.splitlines() if x.startswith("{")]
            wall = time.perf_counter() - t
            cost.refresh()
            assert len(res) == 32 and cost.scan().to_arrow().num_rows == 32 * 77_600
            # different days never conflict logically: every outer retry is
            # PyIceberg running out of its own retries (CommitFailedException)
            assert all(e.startswith("CommitFailedException") for r in res for e in r["errors"])
            runs.append((32 / wall, sum(r["pyiceberg_retries"] for r in res), sum(r["conflicts"] for r in res),
                         len(maintenance.orphan_files(cost, older_than=dt.timedelta(0)))))
            drop_catalog(cat)
            shutil.rmtree(wh, ignore_errors=True)
        med = [statistics.median(x) for x in zip(*runs)]
        lo, hi = min(r[0] for r in runs), max(r[0] for r in runs)
        print(f"{n:7}  {med[0]:5.2f} ({lo:.2f}-{hi:.2f}){'':22}{med[1]:6.0f}  {med[2]:16.0f}  {med[3]:12.0f}")


def kill_loop(exports, runs=30):
    cat, cost, wh = fresh()
    days = gen.days(8)
    base = {d: pq.read_table(f"{exports}/{d}.parquet") for d in days}
    # every version a day could legitimately hold: its billed total
    allowed = defaultdict(set)
    rng = random.Random(7)
    killed = finished = 0
    killed_runs = set()
    for i in range(runs):
        pick = rng.sample(days, 3)
        factor = round(rng.uniform(0.80, 0.99), 4)
        for d in pick:
            allowed[d].add(pc.sum(gen.restate(base[d], factor)["billed_cost"]).as_py())
        p = worker(cat, wh, pick, exports, version=f"r{i}", factor=factor)
        try:
            p.communicate(timeout=rng.uniform(1.0, 5.0))
            finished += 1
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()
            killed += 1
            killed_runs.add(f"r{i}")
        cost.refresh()
        for d in days:
            t = cost.scan(row_filter=ingest.day_filter(d), selected_fields=("billed_cost",)).to_arrow()
            if t.num_rows:
                assert t.num_rows == 77_600, (i, d, t.num_rows)
                assert pc.sum(t["billed_cost"]).as_py() in allowed[d], (i, d)
    landed = defaultdict(int)  # run -> days it committed before it died
    for snap in cost.snapshots():
        bid = snap.summary.additional_properties.get(ingest.BATCH_ID, "") if snap.summary else ""
        if snap.summary and snap.summary.operation.value == "append" and bid:
            landed[bid.rsplit("-", 1)[1]] += 1
    partial = sum(1 for r in killed_runs if 0 < landed[r] < 3)
    none = sum(1 for r in killed_runs if landed[r] == 0)
    orphans = maintenance.orphan_files(cost, older_than=dt.timedelta(0))
    kinds = defaultdict(int)
    for f in orphans:
        kinds["metadata.json" if f.endswith(".metadata.json") else os.path.splitext(f)[1]] += 1
    # Finish the job: one final version of every day, then clean up.
    p = worker(cat, wh, days, exports, version="final", factor=0.95)
    out, err = p.communicate()
    assert p.returncode == 0, err
    removed = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    cost.refresh()
    want = sum((pc.sum(gen.restate(base[d], 0.95)["billed_cost"]).as_py() for d in days), Decimal(0))
    got = duck().execute(f"SELECT count(*), sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()
    assert got == (8 * 77_600, want), got
    assert maintenance.orphan_files(cost, older_than=dt.timedelta(0)) == []
    print(f"\nkill loop: {runs} runs ({killed} killed mid-run, {finished} finished), 8 days checked after every run: "
          f"always one complete version per day, never partial or mixed")
    print(f"of the killed runs: {partial} died after committing 1 or 2 of their 3 days, {none} before committing any, "
          f"{killed - partial - none} after all 3")
    print(f"orphans left by the kills: {len(orphans)} files ({dict(kinds)}); removed {len(removed)} after the final load")
    print(f"final table: {got[0]:,} rows, billed {got[1]} = expected, DuckDB agrees; no orphans left")
    drop_catalog(cat)
    shutil.rmtree(wh, ignore_errors=True)


def main():
    exports = tempfile.mkdtemp(dir=WORK, prefix="exports-")
    try:
        for d in gen.days(32):
            pq.write_table(gen.day_batch(d), f"{exports}/{d}.parquet")
        writers(exports)
        kill_loop(exports)
    finally:
        shutil.rmtree(exports, ignore_errors=True)


if __name__ == "__main__":
    main()
