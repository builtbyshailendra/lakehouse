"""Step 4 measurements.

A. A week of hourly micro-batches (168 commits) in a scratch table: what
   small files and many manifests cost, with and without manifest merging,
   and what compaction gives back. Then a one-resource delete, to see
   copy-on-write amplification.
B. The main table (after scripts/evolution.py): October's 248 small
   team-partitioned files compacted back to one file per day, sorted by team;
   then old snapshots expired and their files cleaned up.
"""

import datetime as dt
import os
import re
import shutil
import statistics
import sys
import time
import uuid

import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import gen, ingest, maintenance, tables  # noqa: E402
from lake.config import WORK, catalog, duck  # noqa: E402
from tests.conftest import TEST_URI, drop_catalog  # noqa: E402

SORT = [("team", "ascending"), ("usage_start", "ascending")]


def median_ms(fn, n=7):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t) * 1000)
    return statistics.median(out)


def profile(c, sql):
    plan = c.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    get = lambda pat: (m.group(1) if (m := re.search(pat, plan)) else "?")  # noqa: E731
    return get(r"Total Files Read: (\d+)"), get(r"Data Read: +([\d.]+ [KMG]?B)"), get(r"Row Groups Scanned: (\d+ / \d+)")


def disk(table):
    files = maintenance.all_files(table)
    data = [f for f in files if f.endswith(".parquet")]
    return len(data), sum(os.path.getsize(f) for f in data), len(files) - len(data), sum(os.path.getsize(f) for f in files if not f.endswith(".parquet"))


def state(label, cost, c):
    m = cost.metadata_location
    week = f"SELECT service, sum(billed_cost) FROM iceberg_scan('{m}') GROUP BY service"
    print(f"  {label}: {len(list(cost.scan().plan_files()))} data files, "
          f"{len(cost.current_snapshot().manifests(cost.io))} manifests, "
          f"metadata.json {os.path.getsize(m) / 1024:.0f} KB; week by service: DuckDB {median_ms(lambda: c.execute(week).fetchall()):.0f} ms, "
          f"PyIceberg plan {median_ms(lambda: list(cost.scan().plan_files())):.0f} ms")


def micro_batches():
    c = duck()
    days = gen.days(7)
    for merge in (False, True):
        name = "mb" + uuid.uuid4().hex[:8]
        wh = f"{WORK}/{name}"
        cat = catalog(name, uri=TEST_URI, warehouse=wh)
        cost = tables.create(cat)[0]
        if merge:
            with cost.transaction() as tx:
                tx.set_properties({"commit.manifest-merge.enabled": "true"})
        t = time.perf_counter()
        for d in days:
            b = ingest.conform(gen.day_batch(d), cost)
            for h in range(24):
                cost.append(b.filter(pc.equal(pc.hour(b["usage_start"]), h)),
                            snapshot_properties={ingest.BATCH_ID: f"cost-{d}T{h:02d}"})
        took = time.perf_counter() - t
        print(f"\n168 hourly appends, manifest merging {'on' if merge else 'off'}: {took:.1f} s ({took / 168 * 1000:.0f} ms per commit)")
        state("before compaction", cost, c)
        if not merge:
            t = time.perf_counter()
            for d in days:
                maintenance.compact(cost, ingest.day_filter(d))
            print(f"  compacted 7 days in {time.perf_counter() - t:.1f} s")
            state("after compaction ", cost, c)

            # copy-on-write delete of one resource's rows
            n = cost.scan(row_filter="resource_id = 'r-00042'").to_arrow().num_rows
            t = time.perf_counter()
            cost.delete("resource_id = 'r-00042'")
            s = cost.current_snapshot().summary
            print(f"  delete one resource ({n} rows of {7 * 77_600:,}): {time.perf_counter() - t:.1f} s, "
                  f"rewrote {s['deleted-data-files']} files, wrote {int(s['added-files-size']) / 1e6:.1f} MB and "
                  f"{int(s['added-records']):,} rows to remove {n}")
        drop_catalog(cat)
        shutil.rmtree(wh, ignore_errors=True)


def dead_manifests(table):
    return sum(1 for m in table.current_snapshot().manifests(table.io) if not m.has_added_files() and not m.has_existing_files())


def dead_manifest_effect():
    """31 days in 4 files each, compacted day by day, then one more append."""
    c = duck()
    name = "dm" + uuid.uuid4().hex[:8]
    wh = f"{WORK}/{name}"
    cat = catalog(name, uri=TEST_URI, warehouse=wh)
    cost = tables.create(cat)[0]
    days = gen.days(32)
    for d in days[:31]:
        b = ingest.conform(gen.day_batch(d), cost)
        for q in range(4):
            cost.append(b.filter(pc.equal(pc.divide(pc.hour(b["usage_start"]), 6), q)))

    def show(label):
        q = f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}') WHERE team = 'payments' AND usage_start < '2026-09-01T00:00:00'"
        ms = cost.current_snapshot().manifests(cost.io)
        print(f"  {label}: {len(list(cost.scan().plan_files()))} files, {len(ms)} manifests ({dead_manifests(cost)} holding only "
              f"deleted entries); one team for August, DuckDB {median_ms(lambda: c.execute(q).fetchall(), 11):.0f} ms")

    print("\ndead manifests: August in 4 files a day, compacted, then one more day appended")
    show("124 small files      ")
    for d in days[:31]:
        maintenance.compact(cost, ingest.day_filter(d))
    show("after compaction     ")
    ingest.load_day(cost, gen.day_batch(days[31]), days[31])
    show("after the next append")
    drop_catalog(cat)
    shutil.rmtree(wh, ignore_errors=True)


def entry_cost():
    """The same 31 days stored as 1, 4 or 16 files a day: what does DuckDB's
    Iceberg reader pay per file (manifest entry), when the query needs one
    day, or nothing at all?"""
    c = duck()
    days = gen.days(31)
    print("\nsame data, more files: DuckDB through Iceberg metadata vs reading the files directly (median of 9)")
    print("  files/day  manifest entries   no row matches   one day (iceberg_scan)   one day (read_parquet)   whole month")
    for per_day in (1, 4, 16):
        name = "ec" + uuid.uuid4().hex[:8]
        wh = f"{WORK}/{name}"
        cat = catalog(name, uri=TEST_URI, warehouse=wh)
        cost = tables.create(cat)[0]
        # PyIceberg splits a write into files by the in-memory size; one
        # append per day keeps one manifest per day, holding per_day entries
        nbytes = ingest.conform(gen.day_batch(days[0]), cost).nbytes
        with cost.transaction() as tx:
            tx.set_properties({"write.target-file-size-bytes": str(nbytes // per_day + 1)})
        for d in days:
            cost.append(ingest.conform(gen.day_batch(d), cost))
        m = cost.metadata_location
        entries = sum(len(mf.fetch_manifest_entry(cost.io)) for mf in cost.current_snapshot().manifests(cost.io))
        day = "usage_start >= '2026-08-15T00:00:00' AND usage_start < '2026-08-16T00:00:00'"
        files = [t.file.file_path for t in cost.scan(row_filter=day).plan_files()]
        none_ = median_ms(lambda: c.execute(f"SELECT sum(billed_cost) FROM iceberg_scan('{m}') WHERE usage_start < '2000-01-01T00:00:00'").fetchall(), 9)
        one = median_ms(lambda: c.execute(f"SELECT sum(billed_cost) FROM iceberg_scan('{m}') WHERE {day}").fetchall(), 9)
        direct = median_ms(lambda: c.execute(f"SELECT sum(billed_cost) FROM read_parquet({files!r}) WHERE {day}").fetchall(), 9)
        month = median_ms(lambda: c.execute(f"SELECT sum(billed_cost) FROM iceberg_scan('{m}')").fetchall(), 9)
        print(f"  {per_day:9}  {entries:16}   {none_:12.0f} ms   {one:19.0f} ms   {direct:20.0f} ms   {month:8.0f} ms")
        drop_catalog(cat)
        shutil.rmtree(wh, ignore_errors=True)


def main_table():
    c = duck()
    cost = catalog().load_table(tables.COST)
    print("\nmain table: October back to one file per day, sorted by team")
    with cost.update_spec() as s:
        s.remove_field("team")
    with cost.transaction() as tx:
        tx.set_properties({"write.parquet.row-group-limit": "10000"})
    t = time.perf_counter()
    before = after = 0
    for d in gen.days(31, dt.date(2026, 10, 1)):
        r = maintenance.compact(cost, ingest.day_filter(d), sort_by=SORT)
        before += r.files_before
        after += r.files_after
    print(f"  compacted {before} files into {after} in {time.perf_counter() - t:.1f} s; "
          f"{dead_manifests(cost)} manifests now hold only deleted entries (cleared by the next load, step 5)")
    m = cost.metadata_location
    for label, rng in (("September (time order)", "usage_start >= '2026-09-01T00:00:00' AND usage_start < '2026-10-01T00:00:00'"),
                       ("October (sorted by team)", "usage_start >= '2026-10-01T00:00:00'")):
        q = f"SELECT sum(effective_cost) FROM iceberg_scan('{m}') WHERE team = 'payments' AND {rng}"
        files, read, rg = profile(c, q)
        print(f"  one team, {label}: {files} files, row groups {rg}, {read} read, {median_ms(lambda: c.execute(q).fetchall()):.0f} ms")

    print("\nmain table: expire snapshots, then remove orphan files")
    total = c.execute(f"SELECT sum(effective_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0]
    n_data, data_bytes, n_meta, meta_bytes = disk(cost)
    print(f"  before: {len(cost.snapshots())} snapshots; on disk {n_data} data files ({data_bytes / 1e6:.0f} MB), "
          f"{n_meta} metadata files ({meta_bytes / 1e6:.1f} MB)")
    t = time.perf_counter()
    removed = maintenance.expire_snapshots(cost, dt.datetime.now(dt.timezone.utc))
    orphans = maintenance.remove_orphan_files(cost, older_than=dt.timedelta(0))
    n_data2, data_bytes2, n_meta2, meta_bytes2 = disk(cost)
    print(f"  expired {removed} snapshots and removed {len(orphans)} orphan files in {time.perf_counter() - t:.1f} s")
    print(f"  after: {len(cost.snapshots())} snapshots kept ({', '.join(sorted(k for k in cost.refs()))}); on disk "
          f"{n_data2} data files ({data_bytes2 / 1e6:.0f} MB), {n_meta2} metadata files ({meta_bytes2 / 1e6:.1f} MB); "
          f"metadata.json {os.path.getsize(cost.metadata_location) / 1024:.0f} KB")
    assert c.execute(f"SELECT sum(effective_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()[0] == total
    sep = cost.refs()["close-2026-09"].snapshot_id
    closed = c.execute(f"SELECT sum(billed_cost) FROM iceberg_scan('{cost.metadata_location}', snapshot_from_id={sep}) "
                       f"WHERE usage_start >= '2026-09-01T00:00:00' AND usage_start < '2026-10-01T00:00:00'").fetchone()[0]
    print(f"  current total unchanged (${total:,.2f}); the September close tag still reads ${closed:,.2f}")


if __name__ == "__main__":
    micro_batches()
    dead_manifest_effect()
    entry_cost()
    main_table()
