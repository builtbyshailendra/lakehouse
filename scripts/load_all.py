"""Step 1 measurements: load 92 days of billing exports, one commit per day,
then look at what Iceberg wrote and check it against the generator with
DuckDB, snapshot by snapshot.

    python scripts/load_all.py            # into the 'lake' catalog
"""

import glob
import os
import shutil
import statistics
import sys
import time
from decimal import Decimal

import pyarrow.compute as pc
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import gen, ingest, meta, tables  # noqa: E402
from lake.config import WAREHOUSE, catalog, duck  # noqa: E402


def best(fn, n=3):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append(time.perf_counter() - t)
    return min(out)


def main():
    cat = catalog()
    with cat.engine.begin() as c:
        c.execute(text("DELETE FROM iceberg_tables WHERE catalog_name = :n"), {"n": cat.name})
        c.execute(text("DELETE FROM iceberg_namespace_properties WHERE catalog_name = :n"), {"n": cat.name})
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    os.makedirs(WAREHOUSE)
    cost, inc = tables.create(cat)
    inc.append(gen.incident_table().cast(inc.schema().as_arrow()))

    truth = []  # (rows, billed) per day from the generator
    commit_s = []
    gen_s = 0.0
    t_all = time.perf_counter()
    for d in gen.days():
        t = time.perf_counter()
        b = gen.day_batch(d)
        gen_s += time.perf_counter() - t
        truth.append((b.num_rows, pc.sum(b["billed_cost"]).as_py()))
        t = time.perf_counter()
        ingest.load_day(cost, b, d)
        commit_s.append(time.perf_counter() - t)
    total_s = time.perf_counter() - t_all
    rows = sum(r for r, _ in truth)
    print(f"loaded {len(commit_s)} days, {rows:,} rows in {total_s:.1f} s "
          f"(generating {gen_s:.1f} s, writing+committing {sum(commit_s):.1f} s, {rows / sum(commit_s):,.0f} rows/s)")
    q = statistics.quantiles(commit_s, n=20)
    print(f"per-day load: median {statistics.median(commit_s) * 1000:.0f} ms, p95 {q[18] * 1000:.0f} ms, "
          f"first 10 avg {statistics.mean(commit_s[:10]) * 1000:.0f} ms, last 10 avg {statistics.mean(commit_s[-10:]) * 1000:.0f} ms")

    t = meta.tree(cost.metadata_location)
    print("\n" + meta.describe(t))
    md = glob.glob(os.path.join(cost.location(), "metadata", "*"))
    md_json = [f for f in md if f.endswith(".metadata.json")]
    print(f"metadata dir: {len(md)} files, {sum(os.path.getsize(f) for f in md):,} bytes "
          f"({len(md_json)} metadata.json kept of {t.snapshots + 1} written)")
    print(f"bytes per row in Parquet (zstd): {t.data_bytes / t.records:.1f}")

    # The same day as CSV, for scale.
    c = duck()
    day = gen.day_batch(gen.START)
    c.register("day", day)
    csv = os.path.join(WAREHOUSE, "day.csv")
    c.execute(f"COPY day TO '{csv}' (HEADER)")
    one = [f for f in t.data_files if "usage_day=2026-08-01" in f["file_path"]][0]
    print(f"one day: CSV {os.path.getsize(csv):,} bytes, Parquet {one['file_size_in_bytes']:,} bytes "
          f"({os.path.getsize(csv) / one['file_size_in_bytes']:.1f}x smaller)")
    os.remove(csv)

    # Every snapshot, read by DuckDB, against the generator.
    ok = 0
    cum_rows, cum_cost = 0, Decimal(0)
    for s, (r, b) in zip(cost.snapshots(), truth):
        cum_rows += r
        cum_cost += b
        got = c.execute(
            f"SELECT count(*), sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}', snapshot_from_id={s.snapshot_id})"
        ).fetchone()
        assert got == (cum_rows, cum_cost), (s.snapshot_id, got, cum_rows, cum_cost)
        ok += 1
    print(f"\nDuckDB vs generator: {ok} of {len(truth)} snapshots identical (row count and billed total to the micro-dollar)")
    print(f"total billed: ${cum_cost:,.2f} (synthetic)")

    m = cost.metadata_location
    one_day = "usage_start >= TIMESTAMP '2026-10-15' AND usage_start < TIMESTAMP '2026-10-16'"
    print("\nqueries (best of 3):")
    for label, sql in [
        ("DuckDB iceberg_scan, whole table", f"SELECT sum(billed_cost) FROM iceberg_scan('{m}')"),
        ("DuckDB iceberg_scan, one day", f"SELECT sum(billed_cost) FROM iceberg_scan('{m}') WHERE {one_day}"),
        ("DuckDB iceberg_scan, by team, whole table",
         f"SELECT team, sum(billed_cost) FROM iceberg_scan('{m}') GROUP BY team"),
    ]:
        print(f"  {label}: {best(lambda: c.execute(sql).fetchall()) * 1000:.0f} ms")
    print(f"  PyIceberg plan, whole table: {best(lambda: list(cost.scan().plan_files())) * 1000:.0f} ms "
          f"({len(list(cost.scan().plan_files()))} files)")
    flt = ingest.day_filter(gen.START.replace(month=10, day=15))
    print(f"  PyIceberg plan, one day: {best(lambda: list(cost.scan(row_filter=flt).plan_files())) * 1000:.0f} ms "
          f"({len(list(cost.scan(row_filter=flt).plan_files()))} file)")
    print(f"  PyIceberg scan to Arrow, one day: {best(lambda: cost.scan(row_filter=flt).to_arrow()) * 1000:.0f} ms")


if __name__ == "__main__":
    main()
