"""Step 5: write-audit-publish with branches and data-quality checks."""

import duckdb
import pyarrow as pa
import pytest

from lake import gen, ingest, quality, wap
from lake.config import duck

DAYS = gen.days(7)


@pytest.fixture
def table(cost):
    for d in DAYS[:4]:
        ingest.load_day(cost, gen.day_batch(d), d)
    return cost


def duck_rows(table):
    return duck().execute(f"SELECT count(*) FROM iceberg_scan('{table.metadata_location}')").fetchone()[0]


def mutate(batch: pa.Table, select: str) -> pa.Table:
    c = duckdb.connect()
    c.register("b", batch)
    return c.sql(select).to_arrow_table()


def test_a_good_export_is_published_and_nobody_sees_it_early(table):
    d = DAYS[4]
    batch = gen.day_batch(d)
    branch, base = wap.stage(table, batch, d, "cost-d5")

    table.refresh()
    assert table.current_snapshot().snapshot_id == base  # main hasn't moved
    assert duck_rows(table) == 4 * 77_600  # DuckDB reads main
    staged = wap.staged_rows(table, branch, base)
    assert staged.num_rows == 77_600

    checks = quality.run(staged, quality.control_for(batch, d), wap.history(table, d))
    assert all(c.passed for c in checks) and len(checks) == 8

    wap.publish(table, branch)
    assert branch not in table.refs()
    assert duck_rows(table) == 5 * 77_600


def _truncated(b):
    return b.slice(0, b.num_rows - 1000)


def _doubled(b):
    return pa.concat_tables([b, b.slice(0, 500)])


# (corruption, does the export manifest describe the bad file?, the check that must fail)
CASES = {
    "truncated transfer": (_truncated, False, "row count matches the export manifest"),
    "part of the file sent twice": (_doubled, False, "line ids unique"),
    # every price x100 and the manifest agrees: only history can tell
    "prices x100 (provider bug)": (lambda b: gen.restate(b, 100, list_too=True), True, "total within 0.5x-2x of the last 7 days' median"),
    "negative usage": (lambda b: mutate(b, "SELECT * REPLACE (CASE WHEN resource_id = 'r-00007' THEN -usage_quantity ELSE usage_quantity END AS usage_quantity) FROM b"), True, "no negative usage"),
    "unknown team": (lambda b: mutate(b, "SELECT * REPLACE (CASE WHEN resource_id = 'r-00007' THEN 'skunkworks' ELSE team END AS team) FROM b"), True, "every team known"),
    "rows from another day": (lambda b: mutate(b, "SELECT * REPLACE (CASE WHEN resource_id = 'r-00007' THEN usage_start - INTERVAL 1 DAY ELSE usage_start END AS usage_start) FROM b"), True, "every row inside the day"),
}


@pytest.mark.parametrize("case", list(CASES))
def test_a_bad_export_is_quarantined_and_main_is_untouched(table, case):
    corrupt, manifest_agrees, must_fail = CASES[case]
    d = DAYS[4]
    good = gen.day_batch(d)
    bad = corrupt(good)
    control = quality.control_for(bad if manifest_agrees else good, d)
    before = table.current_snapshot().snapshot_id

    r = wap.load(table, bad, d, "cost-d5", control)

    assert not r.published
    assert must_fail in [c.name for c in r.failed], r.failed
    table.refresh()
    assert table.current_snapshot().snapshot_id == before
    assert duck_rows(table) == 4 * 77_600
    assert r.branch in table.refs()  # kept for inspection

    # the corrected export goes through
    fixed = wap.load(table, good, d, "cost-d5", quality.control_for(good, d))
    assert fixed.published and not fixed.failed
    assert duck_rows(table) == 5 * 77_600


def test_publish_refuses_to_drop_a_commit_made_to_main_meanwhile(table):
    d5, d6 = DAYS[4], DAYS[5]
    branch, base = wap.stage(table, gen.day_batch(d5), d5, "cost-d5")
    ingest.load_day(table, gen.day_batch(d6), d6)  # someone else commits to main
    with pytest.raises(wap.NotFastForward):
        wap.publish(table, branch)
    table.refresh()
    assert duck_rows(table) == 5 * 77_600  # d6 is there, d5 is not
    r = wap.load(table, gen.day_batch(d5), d5, "cost-d5", quality.control_for(gen.day_batch(d5), d5))
    assert r.published
    assert duck_rows(table) == 6 * 77_600


def test_a_commit_to_main_during_the_audit_makes_wap_stage_again(table, monkeypatch):
    d5, d6 = DAYS[4], DAYS[5]
    run = quality.run
    calls = []

    def run_and_interfere(*a, **k):
        if not calls:
            ingest.load_day(table, gen.day_batch(d6), d6)
        calls.append(1)
        return run(*a, **k)

    monkeypatch.setattr(quality, "run", run_and_interfere)
    r = wap.load(table, gen.day_batch(d5), d5, "cost-d5", quality.control_for(gen.day_batch(d5), d5))
    assert r.published and r.restaged == 1 and len(calls) == 2
    assert duck_rows(table) == 6 * 77_600


def test_the_compare_and_swap_covers_the_gap_after_the_ancestry_check(table, monkeypatch):
    d5, d6 = DAYS[4], DAYS[5]
    branch, base = wap.stage(table, gen.day_batch(d5), d5, "cost-d5")
    check = wap.is_ancestor

    def check_then_interfere(*a):
        ok = check(*a)
        other = table.catalog.load_table(table.name())  # another writer, its own view
        ingest.load_day(other, gen.day_batch(d6), d6)
        return ok

    monkeypatch.setattr(wap, "is_ancestor", check_then_interfere)
    with pytest.raises(wap.NotFastForward, match="branch main has changed"):
        wap.publish(table, branch)
    table.refresh()
    assert duck_rows(table) == 5 * 77_600  # d6 kept, d5 not published
