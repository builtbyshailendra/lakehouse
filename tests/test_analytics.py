"""Step 6: the analytics find what the generator put in."""

import datetime as dt
from decimal import Decimal

import pyarrow.compute as pc
import pytest

from lake import analytics, gen, ingest, tables

N_DAYS = 24


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    """24 days of cost plus the incident log, in a catalog shared by this module."""
    import uuid

    from lake.config import catalog
    from tests.conftest import TEST_URI, drop_catalog

    cat = catalog("a" + uuid.uuid4().hex[:12], uri=TEST_URI, warehouse=str(tmp_path_factory.mktemp("wh")))
    cost, inc = tables.create(cat)
    for d in gen.days(N_DAYS):
        ingest.load_day(cost, gen.day_batch(d), d)
    end = dt.datetime.combine(gen.START + dt.timedelta(days=N_DAYS), dt.time())
    incidents = gen.incident_table()
    incidents = incidents.filter(pc.less(incidents["resolved_at"], end))
    inc.append(incidents.cast(inc.schema().as_arrow()))
    yield cost, inc
    drop_catalog(cat)


@pytest.fixture
def c(lake):
    cost, inc = lake
    return analytics.connect(cost.metadata_location, inc.metadata_location, cost_col="billed_cost")


def test_showback_adds_up_to_the_table_total(c, lake):
    rows = analytics.showback(c)
    assert {r[0] for r in rows} == set(gen.TEAMS)
    assert sum(r[2] for r in rows) == sum((pc.sum(gen.day_batch(d)["billed_cost"]).as_py() for d in gen.days(N_DAYS)), Decimal(0))


def test_spike_detection_finds_every_incident_it_can_and_nothing_else(c):
    analytics.spikes(c)
    found = analytics.incident_cost(c)
    # needs 2 earlier days of the same kind (weekday or weekend) for a baseline
    first_detectable = dt.datetime.combine(gen.START + dt.timedelta(days=9), dt.time())
    eligible = [r for r in found if r[5] >= first_detectable]
    assert len(eligible) >= 4
    assert all(r[6] is not None for r in eligible), [r for r in eligible if r[6] is None]  # every one detected

    # and every spike belongs to an incident: no false alarms
    unmatched = c.execute("""
        SELECT count(*) FROM spikes s WHERE NOT EXISTS (
            SELECT 1 FROM incidents i WHERE s.team = i.team AND s.service = i.service AND s.region = i.region
               AND s.started_at < i.resolved_at AND s.ended_at > i.started_at)""").fetchone()[0]
    assert unmatched == 0


def test_estimated_incident_cost_is_close_to_the_truth(c):
    analytics.spikes(c)
    truth = analytics.true_excess(c, {i.incident_id: i.scale for i in gen.incidents()})
    est = {r[0]: float(r[9]) for r in analytics.incident_cost(c) if r[9] is not None}
    assert est
    for inc_id, value in est.items():
        assert value == pytest.approx(truth[inc_id], rel=0.15), inc_id
