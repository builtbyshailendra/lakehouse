"""Where the time goes in one-team queries on the main table (read-only;
run after scripts/evolution.py): DuckDB through Iceberg metadata vs the same
Parquet files read directly, and PyIceberg."""

import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import tables  # noqa: E402
from lake.config import catalog, duck  # noqa: E402


def med(fn, n=9):
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t) * 1000)
    return statistics.median(ts)


def main():
    cost = catalog().load_table(tables.COST)
    m = cost.metadata_location
    c = duck()
    print("one team, one month: median ms of 9 runs")
    print("            DuckDB iceberg_scan   same files via read_parquet   PyIceberg plan+read")
    for label, rng in (("September", "usage_start >= '2026-09-01T00:00:00' AND usage_start < '2026-10-01T00:00:00'"),
                       ("October", "usage_start >= '2026-10-01T00:00:00'")):
        flt = f"team = 'payments' AND {rng}"
        files = [f.file.file_path for f in cost.scan(row_filter=flt).plan_files()]
        a = med(lambda: c.execute(f"SELECT count(*) FROM iceberg_scan('{m}') WHERE {flt}").fetchall())
        b = med(lambda: c.execute(f"SELECT count(*) FROM read_parquet({files!r}, union_by_name=true) WHERE {flt}").fetchall())
        p = med(lambda: cost.scan(row_filter=flt, selected_fields=("team",)).to_arrow())
        print(f"{label:10}  {a:19.0f}   {b:27.0f}   {p:19.0f}")
    mf = cost.current_snapshot().manifests(cost.io)
    print(f"reading every manifest entry (iceberg_metadata): {med(lambda: c.execute(f'SELECT count(*) FROM iceberg_metadata({chr(39)}{m}{chr(39)})').fetchall()):.0f} ms "
          f"for {len(mf)} manifests")


if __name__ == "__main__":
    main()
