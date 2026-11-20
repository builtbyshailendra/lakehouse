"""Step 5 measurements on the main table (after scripts/maintenance.py):
1-7 November loaded through write-audit-publish, with a truncated transfer,
a x100 provider bug and a commit to main during an audit along the way."""

import datetime as dt
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import gen, ingest, quality, tables, wap  # noqa: E402
from lake.config import catalog, duck  # noqa: E402


def main():
    cost = catalog().load_table(tables.COST)
    c = duck()

    def main_total():
        return c.execute(f"SELECT count(*), sum(effective_cost) FROM iceberg_scan('{cost.metadata_location}')").fetchone()

    def dead():
        return sum(1 for m in cost.current_snapshot().manifests(cost.io) if not m.has_added_files() and not m.has_existing_files())

    oct_q = lambda: (f"SELECT sum(effective_cost) FROM iceberg_scan('{cost.metadata_location}') "  # noqa: E731
                     f"WHERE team = 'payments' AND usage_start >= '2026-10-01T00:00:00' AND usage_start < '2026-11-01T00:00:00'")

    def oct_ms():
        ts = []
        for _ in range(9):
            t = time.perf_counter()
            c.execute(oct_q()).fetchall()
            ts.append((time.perf_counter() - t) * 1000)
        return statistics.median(ts)

    print(f"before: {dead()} dead manifests; one team for October, DuckDB {oct_ms():.0f} ms")
    days = gen.days(7, dt.date(2026, 11, 1))
    good = {d: gen.day_batch(d) for d in days}
    plan = [(days[0], "export", good[days[0]], None)]
    plan += [(days[1], "truncated transfer", good[days[1]].slice(0, 70_000), quality.control_for(good[days[1]], days[1])),
             (days[1], "re-sent", good[days[1]], None)]
    plan += [(days[2], "prices x100 (provider bug)", gen.restate(good[days[2]], 100, list_too=True), None),
             (days[2], "corrected", good[days[2]], None)]
    plan += [(days[3], "export, main changes mid-audit", good[days[3]], None)]
    plan += [(d, "export", good[d], None) for d in days[4:]]

    run = quality.run
    interfere = {"armed": False}

    def run_with_interference(*a, **k):
        if interfere["armed"]:
            interfere["armed"] = False
            d = dt.date(2026, 10, 31)  # a late credit lands on main meanwhile
            ingest.load_day(catalog().load_table(tables.COST), gen.restate(gen.day_batch(d), 0.98), d, batch_id=f"cost-{d}-credit")
        return run(*a, **k)

    quality.run = run_with_interference
    names = ["row count = manifest", "total = manifest", "line ids unique", "no negative usage",
             "billed <= list", "rows inside the day", "teams known", "total vs last 7 days"]
    print("checks: " + ", ".join(f"{i + 1} {n}" for i, n in enumerate(names)))
    print(f"\n{'day':10}  {'what':31}  {'result':22}  {'failed checks':13}  write  audit  publish (ms)")
    quarantined = []
    for i, (d, what, batch, control) in enumerate(plan):
        control = control or quality.control_for(batch, d)
        interfere["armed"] = "mid-audit" in what
        before = main_total()
        r = wap.load(cost, batch, d, f"cost-{d}-{i}", control)
        after = main_total()
        if not r.published:
            assert after == before, "main changed although the audit failed"
            quarantined.append(r.branch)
        failed = ", ".join(str(r.checks.index(x) + 1) for x in r.failed) or "-"
        ms = "  ".join(f"{r.seconds.get(k, 0) * 1000:5.0f}" for k in ("write", "audit", "publish"))
        res = ("published" + (f", restaged {r.restaged}x" if r.restaged else "")) if r.published else "QUARANTINED"
        print(f"{d}  {what:31}  {res:22}  {failed:13}  {ms}")
        for x in r.failed:
            print(f"{'':12}  check {r.checks.index(x) + 1} failed: {x.name} ({x.detail})")
    quality.run = run

    rows, total = main_total()
    print(f"\nmain now: {rows:,} rows (99 days), total ${total:,.2f}; DuckDB never saw a quarantined batch "
          f"(main's totals were unchanged across each failed audit)")
    print(f"quarantined branches kept for inspection: {', '.join(sorted(quarantined))}")
    for b in quarantined:
        cost.manage_snapshots().remove_branch(b).commit()
    print(f"after review, branches removed; refs now: {', '.join(sorted(cost.refs()))}")
    print(f"after the first publish: {dead()} dead manifests; one team for October, DuckDB {oct_ms():.0f} ms")


if __name__ == "__main__":
    main()
