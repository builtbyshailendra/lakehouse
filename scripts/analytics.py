"""Step 6 measurements: the FinOps questions on the main table (after
scripts/wap.py), scored against the incidents the generator injected."""

import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lake import analytics, gen, tables  # noqa: E402
from lake.config import catalog  # noqa: E402


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, (time.perf_counter() - t) * 1000


def main():
    cat = catalog()
    cost, inc = cat.load_table(tables.COST), cat.load_table(tables.INCIDENTS)
    c = analytics.connect(cost.metadata_location, inc.metadata_location)

    rows, ms = timed(lambda: analytics.showback(c))
    months = sorted({r[1] for r in rows})
    print(f"showback: cost per team per month ({ms:.0f} ms; synthetic dollars; November is 1-7 only)")
    print("  team             " + "".join(f"{m:%b %Y}".rjust(16) for m in months) + "   Oct vs Sep")
    by = {(r[0], r[1]): r for r in rows}
    for team in sorted(gen.TEAMS, key=lambda t: -float(by[(t, months[2])][2])):
        cells = "".join(f"{float(by[(team, m)][2]):16,.0f}" for m in months)
        print(f"  {team:15}{cells}   {by[(team, months[2])][3]:+.1f}%")

    _, ms = timed(lambda: analytics.spikes(c))
    n_spikes = c.execute("SELECT count(*) FROM spikes").fetchone()[0]
    found = analytics.incident_cost(c)
    truth = analytics.true_excess(c, {i.incident_id: i.scale for i in gen.incidents()})
    detected = [r for r in found if r[6] is not None]
    missed = [r for r in found if r[6] is None]
    unmatched = c.execute("""SELECT count(*) FROM spikes s WHERE NOT EXISTS (SELECT 1 FROM incidents i
        WHERE s.team = i.team AND s.service = i.service AND s.region = i.region
          AND s.started_at < i.resolved_at AND s.ended_at > i.started_at)""").fetchone()[0]
    print(f"\nspike detection ({ms:.0f} ms over {cost.current_snapshot().summary['total-records']} rows): "
          f"{n_spikes} spike events; {len(detected)} of {len(found)} incidents found, {unmatched} spikes with no incident")
    for r in missed:
        print(f"  missed {r[0]} on {r[5]:%d %b %H:%M}: " +
              ("too early: under 2 earlier days of the same kind (weekday/weekend) for a baseline"
               if (r[5].date() - gen.START).days < 9 else "below the threshold"))
    errs = [abs(float(r[9]) - truth[r[0]]) / truth[r[0]] * 100 for r in detected]
    print(f"  estimated incident cost vs the truth: median error {statistics.median(errs):.1f}%, worst {max(errs):.1f}%")
    print("  costliest incidents (estimate / truth):")
    for r in sorted(detected, key=lambda r: -float(r[9]))[:5]:
        print(f"    {r[0]} {r[1]} {r[2]:13} {r[3]:9} {r[4]:10} {r[5]:%d %b %H:%M}  {r[7]}h at {r[8]}x  "
              f"${float(r[9]):,.2f} / ${truth[r[0]]:,.2f}")


if __name__ == "__main__":
    main()
