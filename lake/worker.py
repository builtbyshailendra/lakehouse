"""A loader process, for the crash and concurrency tests.

    python -m lake.worker --catalog NAME --uri URI --warehouse DIR \\
        --days 2026-08-01,2026-08-02 [--version v2 --factor 0.9] [--from-dir DIR]

Prints one JSON line per day. LAKE_CRASH (see faults.py) makes it die at a
chosen point of the commit.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time

import pyarrow.parquet as pq

from . import faults, gen, ingest, tables
from .config import catalog


class _CountRetries(logging.Handler):
    """Counts PyIceberg's own commit retries (it logs each one)."""

    def __init__(self):
        super().__init__()
        self.n = 0

    def emit(self, record):
        if "retrying" in record.getMessage():
            self.n += 1


def batch_for(day: dt.date, factor: float, from_dir: str | None):
    if from_dir:
        b = pq.read_table(os.path.join(from_dir, f"{day}.parquet"))
    else:
        b = gen.day_batch(day)
    return b if factor == 1.0 else gen.restate(b, factor)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--uri", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--days", required=True)
    p.add_argument("--version", default="")
    p.add_argument("--factor", type=float, default=1.0)
    p.add_argument("--from-dir")
    a = p.parse_args(argv)

    faults.install()
    retries = _CountRetries()
    logging.getLogger("pyiceberg.table").addHandler(retries)
    logging.getLogger("pyiceberg.table").setLevel(logging.WARNING)
    logging.getLogger("pyiceberg.table").propagate = False

    cost = catalog(a.catalog, uri=a.uri, warehouse=a.warehouse).load_table(tables.COST)
    for s in a.days.split(","):
        day = dt.date.fromisoformat(s)
        b = batch_for(day, a.factor, a.from_dir)
        bid = f"cost-{day}" + (f"-{a.version}" if a.version else "")
        before = retries.n
        t = time.perf_counter()
        r = ingest.load_day(cost, b, day, batch_id=bid)
        print(json.dumps({
            "day": str(day), "batch_id": bid, "skipped": r.skipped, "conflicts": r.conflicts,
            "pyiceberg_retries": retries.n - before, "errors": r.conflict_errors, "seconds": round(time.perf_counter() - t, 3),
        }), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
