"""Step 3 measurements on the main table (run scripts/load_all.py first).

Month-end tags, a rename on 7.1 million rows, September credits after the
close, a bad load rolled back, and October re-laid out by team.
"""

import datetime as dt
import os
import re
import shutil
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import evolution, gen, ingest, tables  # noqa: E402
from lake.config import WAREHOUSE, catalog, duck  # noqa: E402


def ms(t):
    return f"{(time.perf_counter() - t) * 1000:.0f} ms"


def profile(c, sql):
    """Files and bytes DuckDB actually read, from its own profile."""
    plan = c.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    files = re.search(r"Total Files Read: (\d+)", plan).group(1)
    read = re.search(r"Data Read: +([\d.]+ [KMG]?B)", plan).group(1)
    return files, read


def median_ms(fn, n=9):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t) * 1000)
    return statistics.median(out)


def main():
    cost = catalog().load_table(tables.COST)
    c = duck()

    def total(where="TRUE", snap=None, col="effective_cost"):
        s = f", snapshot_from_id={snap}" if snap else ""
        return c.execute(f"SELECT sum({col}) FROM iceberg_scan('{cost.metadata_location}'{s}) WHERE {where}").fetchone()[0]

    for month, last in (("2026-08", dt.date(2026, 8, 31)), ("2026-09", dt.date(2026, 9, 30))):
        evolution.tag(cost, f"close-{month}", evolution.snapshot_after_day(cost, last))
    print("tags:", sorted(k for k in cost.refs() if k != "main"))

    # 1. rename + add column on the whole table
    before = {f.file.file_path: os.path.getmtime(f.file.file_path) for f in cost.scan().plan_files()}
    total_before = total(col="billed_cost")
    t = time.perf_counter()
    evolution.adopt_focus_names(cost)
    took = ms(t)
    after = {f.file.file_path: os.path.getmtime(f.file.file_path) for f in cost.scan().plan_files()}
    assert after == before and total() == total_before
    print(f"\nrename billed_cost -> effective_cost + add commitment_discount_id: {took}, "
          f"{len(before)} data files untouched, total unchanged (${total_before:,.2f})")
    out = f"{WAREHOUSE}/rewrite-test"
    shutil.rmtree(out, ignore_errors=True)
    t = time.perf_counter()
    c.execute(f"COPY (SELECT * RENAME (effective_cost AS ec) FROM iceberg_scan('{cost.metadata_location}')) "
              f"TO '{out}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (team))")
    rows = int(cost.current_snapshot().summary["total-records"])
    print(f"for scale, rewriting all {rows:,} rows with DuckDB instead: {ms(t)}")
    shutil.rmtree(out, ignore_errors=True)

    # 2. credits on ten September days after the close
    # ISO strings work in both DuckDB SQL and PyIceberg's filter parser
    sep = "usage_start >= '2026-09-01T00:00:00' AND usage_start < '2026-10-01T00:00:00'"
    closed = cost.refs()["close-2026-09"].snapshot_id
    sep_closed = total(sep, closed, col="billed_cost")
    for d in gen.days(10, dt.date(2026, 9, 1)):
        ingest.load_day(cost, gen.restate(gen.day_batch(d), 0.97), d, batch_id=f"cost-{d}-credit")
    print(f"\nSeptember at close (tag close-2026-09): ${sep_closed:,.2f}; after 3% credits on 1-10 Sep: "
          f"${total(sep):,.2f}; the tag still reads ${total(sep, closed, col='billed_cost'):,.2f}")

    # 3. a bad load, rolled back
    good = cost.current_snapshot().snapshot_id
    last = dt.date(2026, 10, 31)
    ingest.load_day(cost, gen.restate(gen.day_batch(last), 100), last, batch_id=f"cost-{last}-bad")
    day = "usage_start >= '2026-10-31T00:00:00'"
    bad = total(day)
    t = time.perf_counter()
    evolution.rollback(cost, good)
    took = ms(t)
    print(f"\nbad load of 31 Oct (x100): day total ${bad:,.2f}; rollback took {took}; day total back to ${total(day):,.2f}")

    # 4. partition evolution: October re-laid out by team
    evolution.partition_by_team(cost)
    t = time.perf_counter()
    for d in gen.days(31, dt.date(2026, 10, 1)):
        ingest.load_day(cost, gen.day_batch(d), d, batch_id=f"cost-{d}-by-team")
    print(f"\npartitioned by day + team from now on; re-laid out October (31 days) in {(time.perf_counter() - t):.1f} s")
    files = list(cost.scan().plan_files())
    for label, spec in (("Aug-Sep (day)", 0), ("Oct (day + team)", 1)):
        fs = [f.file for f in files if f.file.spec_id == spec]
        print(f"  {label}: {len(fs)} files, avg {sum(f.file_size_in_bytes for f in fs) / len(fs) / 1e6:.2f} MB")
    oct_ = "usage_start >= '2026-10-01T00:00:00'"
    m = cost.metadata_location
    print("queries, DuckDB (files and bytes from EXPLAIN ANALYZE; median of 9 runs):")
    for what, team in (("one team", "team = 'payments' AND "), ("all teams", "")):
        for label, rng in (("September", sep), ("October", oct_)):
            q = f"SELECT team, sum(effective_cost) FROM iceberg_scan('{m}') WHERE {team}{rng} GROUP BY team"
            files, read = profile(c, q)
            print(f"  {what}, {label}: {files} files, {read} read, {median_ms(lambda: c.execute(q).fetchall()):.0f} ms")
    print(f"\nsnapshots: {len(cost.snapshots())}, metadata.json {os.path.getsize(cost.metadata_location):,} bytes")


if __name__ == "__main__":
    main()
