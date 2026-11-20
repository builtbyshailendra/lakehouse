"""Step 6b: the same workloads on Iceberg (PyIceberg, SQL catalog) and on
DuckLake (DuckDB's table format: metadata in SQL tables, data in Parquet),
both with their catalog in the same local PostgreSQL.

    A. 31 daily loads, each replacing its day (delete + insert, one commit)
    B. queries over the result
    C. metadata footprint
    D. a week of hourly appends (168 small commits), with and without
       DuckLake's data inlining (small inserts stored in the catalog database)
"""

import os
import shutil
import statistics
import subprocess
import sys
import time
import uuid

import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import gen, ingest, maintenance, tables  # noqa: E402
from lake.config import DUCKDB_EXTENSIONS, WORK, catalog, duck  # noqa: E402
from tests.conftest import TEST_URI, drop_catalog  # noqa: E402

PSQL = [os.environ.get("LAKE_PSQL") or shutil.which("psql") or "D:/DB/bin/psql.exe", "-h", "127.0.0.1", "-p", "5433", "-U", "postgres", "-d", "postgres", "-tAc"]
DL_DB = "ducklake_test"


def psql(sql):
    return subprocess.run(PSQL + [sql], capture_output=True, text=True, check=True).stdout.strip()


def ms_stats(xs):
    return f"median {statistics.median(xs) * 1000:.0f} ms, p95 {statistics.quantiles(xs, n=20)[18] * 1000:.0f} ms"


def hourly(batch):
    return [batch.filter(pc.equal(pc.hour(batch["usage_start"]), h)) for h in range(24)]


def ducklake(data_path, inlining):
    psql(f"DROP DATABASE IF EXISTS {DL_DB} WITH (FORCE)")
    psql(f"CREATE DATABASE {DL_DB}")
    shutil.rmtree(data_path, ignore_errors=True)
    os.makedirs(data_path)
    c = duck(("iceberg", "avro", "ducklake", "postgres"))
    c.execute(f"ATTACH 'ducklake:postgres:dbname={DL_DB} host=127.0.0.1 port=5433 user=postgres' AS dl "
              f"(DATA_PATH '{data_path}/', DATA_INLINING_ROW_LIMIT {inlining})")
    c.execute("""CREATE TABLE dl.cost (
        line_id VARCHAR, usage_start TIMESTAMP, account_id VARCHAR, team VARCHAR, service VARCHAR, region VARCHAR,
        resource_id VARCHAR, usage_type VARCHAR, usage_quantity DOUBLE, unit VARCHAR,
        list_cost DECIMAL(14, 6), billed_cost DECIMAL(14, 6))""")
    c.execute("ALTER TABLE dl.cost SET PARTITIONED BY (year(usage_start), month(usage_start), day(usage_start))")
    return c


def iceberg():
    name = "dk" + uuid.uuid4().hex[:8]
    wh = f"{WORK}/{name}"
    cat = catalog(name, uri=TEST_URI, warehouse=wh)
    return cat, tables.create(cat)[0], wh


def main():
    days = gen.days(31)
    batches = {d: gen.day_batch(d) for d in days}
    week = days[:7]

    # A. daily loads
    print("A. 31 daily loads, each replacing its day")
    cat, cost, wh = iceberg()
    ice = []
    for d in days:
        t = time.perf_counter()
        ingest.load_day(cost, batches[d], d)
        ice.append(time.perf_counter() - t)
    c = ducklake(f"{WORK}/ducklake", 0)
    dl = []
    for d in days:
        c.register("b", batches[d])
        t = time.perf_counter()
        c.execute("BEGIN")
        c.execute(f"DELETE FROM dl.cost WHERE usage_start >= '{d}' AND usage_start < '{d}'::DATE + 1")
        c.execute("INSERT INTO dl.cost SELECT * FROM b")
        c.execute("COMMIT")
        dl.append(time.perf_counter() - t)
        c.unregister("b")
    print(f"  Iceberg (PyIceberg): {ms_stats(ice)}")
    print(f"  DuckLake (DuckDB):   {ms_stats(dl)}")
    # the same day again (a restatement), replacing data that is there
    d = days[15]
    re_b = gen.restate(batches[d], 0.97)
    t = time.perf_counter()
    ingest.load_day(cost, re_b, d, batch_id="restate")
    t_ice = time.perf_counter() - t
    c.register("b", re_b)
    t = time.perf_counter()
    c.execute("BEGIN")
    c.execute(f"DELETE FROM dl.cost WHERE usage_start >= '{d}' AND usage_start < '{d}'::DATE + 1")
    c.execute("INSERT INTO dl.cost SELECT * FROM b")
    c.execute("COMMIT")
    t_dl = time.perf_counter() - t
    print(f"  restating one loaded day: Iceberg {t_ice * 1000:.0f} ms, DuckLake {t_dl * 1000:.0f} ms")

    # B. queries on the month
    m = cost.metadata_location
    queries = {
        "month by service": ("SELECT service, sum(billed_cost) FROM {t} GROUP BY service", ),
        "one day by team": ("SELECT team, sum(billed_cost) FROM {t} WHERE usage_start >= '2026-08-20' AND usage_start < '2026-08-21' GROUP BY team", ),
    }
    print("B. queries (DuckDB both times; median of 9)")
    ci = duck()
    for label, (q,) in queries.items():
        a = ci.execute(q.format(t=f"iceberg_scan('{m}')")).fetchall()
        b = c.execute(q.format(t="dl.cost")).fetchall()
        assert sorted(a) == sorted(b), (label, a, b)
        ti = []
        td = []
        for _ in range(9):
            t = time.perf_counter()
            ci.execute(q.format(t=f"iceberg_scan('{m}')")).fetchall()
            ti.append(time.perf_counter() - t)
            t = time.perf_counter()
            c.execute(q.format(t="dl.cost")).fetchall()
            td.append(time.perf_counter() - t)
        print(f"  {label}: Iceberg {statistics.median(ti) * 1000:.0f} ms, DuckLake {statistics.median(td) * 1000:.0f} ms (same answer)")

    # C. metadata
    md = [f for f in maintenance.all_files(cost) if not f.endswith(".parquet")]
    print("C. metadata after 32 commits")
    print(f"  Iceberg: {len(md)} files, {sum(os.path.getsize(f) for f in md) / 1e6:.1f} MB "
          f"(metadata.json {os.path.getsize(m) / 1024:.0f} KB)")
    print(f"  DuckLake: catalog database {int(psql(f'SELECT pg_database_size({chr(39)}{DL_DB}{chr(39)})')) / 1e6:.1f} MB "
          f"(an empty PostgreSQL database is about 7.5 MB)")
    c.close()
    drop_catalog(cat)
    shutil.rmtree(wh, ignore_errors=True)

    # D. hourly appends
    print("D. a week of hourly appends (168 commits of ~3,200 rows)")
    cat, cost, wh = iceberg()
    ice = []
    for d in week:
        for part in hourly(ingest.conform(batches[d], cost)):
            t = time.perf_counter()
            cost.append(part)
            ice.append(time.perf_counter() - t)
    print(f"  Iceberg: {ms_stats(ice)}; {len(list(cost.scan().plan_files()))} data files")
    for inlining in (0, 10_000):
        c = ducklake(f"{WORK}/ducklake", inlining)
        dl = []
        for d in week:
            for part in hourly(batches[d]):
                c.register("b", part)
                t = time.perf_counter()
                c.execute("INSERT INTO dl.cost SELECT * FROM b")
                dl.append(time.perf_counter() - t)
                c.unregister("b")
        files = sum(len(fs) for _, _, fs in os.walk(f"{WORK}/ducklake"))
        label = "DuckLake" + (f", inlining up to {inlining:,} rows" if inlining else ", no inlining")
        q = "SELECT service, sum(billed_cost) FROM dl.cost GROUP BY service"
        tq = []
        for _ in range(9):
            t = time.perf_counter()
            c.execute(q).fetchall()
            tq.append(time.perf_counter() - t)
        print(f"  {label}: {ms_stats(dl)}; {files} data files; week by service {statistics.median(tq) * 1000:.0f} ms")
        if inlining:
            t = time.perf_counter()
            c.execute("CALL ducklake_flush_inlined_data('dl')")
            files = sum(len(fs) for _, _, fs in os.walk(f"{WORK}/ducklake"))
            print(f"    flushing the inlined rows to Parquet: {(time.perf_counter() - t) * 1000:.0f} ms, {files} files")
        c.close()
    drop_catalog(cat)
    shutil.rmtree(wh, ignore_errors=True)
    shutil.rmtree(f"{WORK}/ducklake", ignore_errors=True)
    psql(f"DROP DATABASE IF EXISTS {DL_DB} WITH (FORCE)")
    print(f"\n(DuckDB extensions from {DUCKDB_EXTENSIONS or '~/.duckdb'}; both catalogs on the same PostgreSQL 16)")


if __name__ == "__main__":
    main()
