# lakehouse

A small lakehouse on **Apache Iceberg**: PyIceberg 0.12 writes the tables, the catalog lives in a local PostgreSQL, and **DuckDB 2.0** reads them. The data is three months of synthetic cloud billing exports (7.1 million line items in the spirit of the FinOps FOCUS format) plus an incident log. Built step by step to see how a table format behaves under crashes, racing writers, schema changes, small files, bad data and analytics, measuring each step.

Built pairing with Claude Code. Everything runs locally; all data is synthetic (made-up teams, resources, costs and incidents). Not published.

```text
billing export (Arrow) ──► write-audit-publish ──► branch audit-<batch> ──► checks ──► fast-forward main
                                                                      └─ failed: quarantined, main untouched
PostgreSQL (SQL catalog): one row per table ──► metadata.json ──► manifest list ──► manifests ──► Parquet (zstd)
DuckDB iceberg_scan ◄── the same files ──► PyIceberg scans
maintenance: compaction (file-level replace) · snapshot expiry · orphan cleanup (age-limited)
```

## Run it

```bash
python -m venv D:/cache/venv-lakehouse
D:/cache/venv-lakehouse/Scripts/python -m pip install "pyiceberg[sql-postgres,pyarrow,duckdb,pyiceberg-core]==0.12.0" duckdb==2.0.0 pyarrow==26.0.1 pytest==9.2.0
D:/cache/venv-lakehouse/Scripts/python -m pytest              # 47 tests, about 3 minutes
D:/cache/venv-lakehouse/Scripts/python scripts/run_all.py     # every measurement below, about 15 minutes
D:/cache/venv-lakehouse/Scripts/python -m lake show           # the cost table's metadata tree
```
Needs PostgreSQL with databases `lakehouse` and `lakehouse_test` (here the test cluster on port 5433, trust auth on localhost) and DuckDB's `iceberg`, `avro`, `ducklake` and `postgres` extensions (installed once into `LAKE_DUCKDB_EXT`). Settings: `LAKE_PG_URI`, `LAKE_TEST_PG_URI`, `LAKE_WAREHOUSE` in `lake/config.py`. On Windows, PyIceberg rejects `file:///D:/...` locations; plain `D:/...` works for both engines.

## The data

`lake/gen.py`, deterministic: 4,000 resources owned by 8 teams across compute, database, storage, network, ML and observability in 4 regions. One line item per resource per hour (storage once a day): **77,600 rows a day**. Usage follows a daily curve, drops 20% at weekends, grows 0.3% a day, and carries ±10% noise from a hash. Production compute and database get a 28% committed-use discount. Money is `DECIMAL(14,6)`, so totals reconcile to the micro-dollar. 27 incidents scale up one team's compute, database or network in one region for 1–3 hours (2.5×–5×), so the analytics have something real to find.

## Step 1: tables, a PostgreSQL catalog, and a second engine

The cost table is partitioned by `day(usage_start)` (hidden partitioning: queries filter on `usage_start`). Each daily export is loaded by **replacing its day** in one commit, because billing data is restated; the batch id goes into the snapshot summary.

| Measured (`results/step1.txt`) | |
|---|---|
| 92 daily loads, 7,139,200 rows | 54.8 s (generating 9.7 s; writing and committing 45.1 s, 158,365 rows/s); per load median 416 ms, p95 1,053 ms |
| Data | 92 Parquet files, 167 MB, **23.4 bytes per row** (zstd); one day as CSV is 11.6 MB vs 1.8 MB (6.4×) |
| Metadata after 92 commits | metadata.json 75 KB; 92 manifests in the current snapshot (one per commit); 235 metadata files, 3.8 MB; 51 of 93 metadata.json kept (`previous-versions-max` 50, older ones deleted after commit) |
| Pruning | one day: PyIceberg plans 1 file in 6 ms (whole table: 92 files, 179 ms); DuckDB sums one day in 28 ms vs 186 ms for the table |

DuckDB's iceberg extension read every one of the 92 snapshots and matched the generator's row count and billed total exactly. `lake/meta.py` reads the metadata tree by hand (JSON with the standard library, Avro manifests with DuckDB) and its totals match the table's.

## Step 2: crashes, retries and racing writers

`lake/faults.py` kills the loader (`os._exit(137)`) at three points of a commit; `tests/test_commits.py`:

| Killed | Table afterwards | Left behind |
|---|---|---|
| after writing data files and manifests | unchanged | Parquet, manifest, manifest list (orphans) |
| after writing the new metadata.json | unchanged | the above plus a metadata.json |
| after the catalog swap, before the answer | committed | nothing; the rerun finds its batch id and skips |

- **Two writers replacing the same new day:** under *serializable* isolation (the default) the second is refused (`ValidationException`) and the loader re-reads and retries; under *snapshot* isolation **both commit and the day is there twice** (155,200 rows instead of 77,600).
- **PyIceberg leaves orphans even without crashes:** a refused commit deletes its manifests but not its Parquet files.
- **Orphan cleanup with no age limit breaks a running write** (shown with a writer paused just before its commit: the commit succeeds and the table points at a deleted file). With a 1-hour limit the same run is fine.

Measured (`scripts/concurrency.py`): 1, 2, 4 and 8 loader processes loading 32 different days between them, 3 runs each.

| Writers | Commits/s, median (range) | PyIceberg retries | Loader retries (PyIceberg gave up) | Orphan files left |
|---|---|---|---|---|
| 1 | 2.36 (2.31–3.36) | 0 | 0 | 0 |
| 2 | 2.85 (2.68–4.08) | 18 | 0 | 8 |
| 4 | 3.67 (3.39–3.68) | 33 | 1 | 4 |
| 8 | 2.83 (2.67–3.00) | 61 | 5 | 21 |

No clear gain from more writers (the ranges overlap; earlier runs put 1 writer at 3.2–3.5), while retries climb, and every loader retry was PyIceberg running out of its 5 attempts, never a logical conflict: one table takes one commit at a time.

**Kill loop:** 30 runs of a loader restating 3 random days, killed at a random moment (18 killed mid-run: 7 after committing 1 or 2 of their days, 9 before any). After every run all 8 days were checked: always one complete version per day, never partial or mixed. The kills left 15 orphans (4 Parquet, 11 Avro); after a final load and cleanup the table held exactly the expected 620,800 rows and total, and DuckDB agreed.

## Step 3: evolution, time travel, tags, rollback

On the main table (`scripts/evolution.py`, `scripts/query_overhead.py`):
- **Rename** `billed_cost` → `effective_cost` (FOCUS 1.0's name) plus a new `commitment_discount_id`: **40 ms**, all 92 data files untouched, total unchanged. Rewriting the 7.1 M rows with DuckDB instead: 6.5 s.
- **Month-end tags:** `close-2026-09` reads $4,080,894.02. After 3% credits on 1–10 September the current total is $4,040,629.36; the tag still reads $4,080,894.02.
- **Rollback:** a ×100 load of 31 October ($12,968,296.95 for the day) rolled back in one metadata commit (564 ms in this run, 57–392 ms in earlier ones); the day is back to $129,682.97, and a corrected load of the same batch id goes in.
- **Partition evolution:** from now on `day + team`; October re-laid out (24.6 s) became 248 files of 0.23 MB against 1.82 MB per day file before.

| DuckDB, median of 9 | Files | Read | Time |
|---|---|---|---|
| one team, September (day files) | 30 | 13.1 MB | 88 ms |
| one team, October (day + team) | 31 | 2.0 MB | 179 ms |
| all teams, September | 30 | 13.1 MB | 90 ms |
| all teams, October | 248 | 16.9 MB | 284 ms |

6.5× fewer bytes and **twice as slow**. The same 31 October files read directly with `read_parquet` took 18 ms (September 17 ms); through `iceberg_scan` 173 ms (September 80 ms). The difference is Iceberg metadata, not data: October's manifests list 8 files each, and DuckDB decodes every entry it has to consider (step 4 measures this).

- Field IDs keep the rename and a dropped-then-re-added column right in both PyIceberg and DuckDB. Reading the same Parquet files **by name** (DuckDB `read_parquet`, `union_by_name`) shows the failure modes Iceberg avoids: after the rename, half the costs are under each name; after drop and re-add, the dropped values come back.
- Time travel returns the rows **and the schema** of the time (the old column name), in both engines.
- Unsafe changes are refused: string → long, decimal narrowing, adding a required column.

## Step 4: small files, compaction, deletes, expiry

`scripts/maintenance.py`:

| A week loaded as 168 hourly appends | Files | Manifests | DuckDB, week by service | PyIceberg planning |
|---|---|---|---|---|
| as loaded (227 ms per commit) | 168 | 168 | 188 ms | 309 ms |
| with manifest merging on | 168 | 69 | 287 ms | 187 ms |
| after compacting each day (5.0 s) | 7 | 14 | 150 ms | 35 ms |

- **Copy-on-write delete:** removing one resource (168 rows of 543,200) rewrote all 7 files: **543,032 rows, 13.2 MB written to delete 168 rows** (2.0 s). A delete aligned with partitions rewrites nothing.
- **What DuckDB pays for:** the same 31 days stored as 1, 4 or 16 files a day. A query for the whole month took 140, 281 and 663 ms; a one-day query, which prunes to one manifest, stayed at 13–25 ms; a query matching nothing, 3–7 ms. Time follows the files a query has to consider, not the bytes.
- **Dead manifests:** August in 4 files a day (124 files): one-team query 335 ms. After compacting each day: 31 files but 62 manifests, 31 of them listing only deleted files: 362 ms. After the next ordinary append drops them: **136 ms**.
- **October fixed by compaction instead of partitioning:** the team field removed from the spec, October's 248 files compacted into 31, sorted by team, row groups of 10,000 rows. A one-team query read 62 of 248 row groups (4.2 MB) instead of 248 files. Right after compaction it took 573 ms because of the 31 dead manifests; after the next load (step 5) 56 ms.
- **Expiry and cleanup:** 207 snapshots → 3 (the two close tags and main's head) and 589 orphan files removed in 1.0 s: data on disk from 382 files (300 MB) to 102 files (185 MB) (the 92 current days plus the 10 pre-credit September files the close tag still needs), metadata.json from 137 KB to 13 KB. The current total and the September close tag read the same as before.

Compaction (`maintenance.compact`) is a **file-level replace committed as an overwrite**, and the tests show why each part matters:
- an append that lands during compaction is kept (PyIceberg refuses the first attempt under serializable isolation, since it counts any concurrent append as a conflict; the retry includes the new file);
- a restatement that lands during compaction makes it plan again (under either isolation level) instead of bringing old rows back;
- done as delete + append instead, a consumer reading new appends would get the compacted rows again (155,200 instead of 77,600).

## Step 5: write-audit-publish

Each export is written to a branch made from main, audited there, and published by fast-forwarding main. The audit reads exactly the rows the branch appended (an incremental scan from the branch's base), so rows dated on another day can't hide in another partition. Checks: row count and total against the export's manifest, unique line ids, no negative usage, billed never above list, every row inside the day, known teams, and the day's total within 0.5×–2× of the last 7 days' median.

1–7 November through write-audit-publish on the main table (`scripts/wap.py`):

| Day | Export | Result | Failed check |
|---|---|---|---|
| 1 Nov | as sent | published | |
| 2 Nov | truncated transfer (70,000 of 77,600 rows) | quarantined | row count and total against the export manifest |
| 2 Nov | re-sent | published | |
| 3 Nov | every price ×100, manifest agrees (provider bug) | quarantined | only the history check: 101× the 7-day median |
| 3 Nov | corrected | published | |
| 4 Nov | as sent, while a late credit is committed to main mid-audit | published after staging again once | |
| 5–7 Nov | as sent | published | |

Main's row count and total were checked before and after each failed audit and never changed, so DuckDB never saw a quarantined batch. Writing a day to its branch took 0.3–1.8 s, the audit 0.2–0.4 s, publishing 0.06–0.8 s. The quarantined branches stayed until removed after review.

`tests/test_wap.py` covers six kinds of bad export (each quarantined by the check that should catch it, main untouched, then the corrected export published), a commit to main between staging and publishing (refused, staged again), and a commit landing between the ancestry check and the publish (the catalog's compare-and-swap refuses it).

## Step 6: analytics, and the same workload on DuckLake

`lake/analytics.py`, DuckDB SQL over the Iceberg tables (`scripts/analytics.py`):
- **Showback:** cost per team per month with month-over-month change (synthetic: every team grew about 12% from September to October).
- **Spike detection:** hourly cost per team, service and region against the median of the same hour on the same kind of day (weekday or weekend) over the previous 14 days; 1.8× or more is a spike; consecutive hours merge into events (gaps and islands).
- **Incident cost:** spikes joined to the incident log.

| Against what the generator injected | |
|---|---|
| Incidents found | **27 of 27**, 0 spikes with no incident |
| Incident cost estimated vs the truth | median error 0.9%, worst 3.7% |
| Costliest | INC-0025, search network in ap-south-1, 3 h at 3.72×: $705.97 estimated, $698.42 true |

The same workload on **DuckLake** (DuckDB's table format: metadata in SQL tables, data in Parquet), its catalog on the same PostgreSQL (`scripts/ducklake.py`):

| | Iceberg (PyIceberg) | DuckLake |
|---|---|---|
| 31 daily loads replacing their day, median (p95) | 344 ms (813) | 280 ms (909) |
| restating one loaded day | 595 ms | 732 ms |
| 168 hourly appends, median (p95) | 184 ms (567) | **25 ms (40)** |
| hourly appends with inlining up to 10,000 rows | | 109 ms (358), no files, but the week's query took 692 ms instead of 32 ms; flushing to Parquet took 4.0 s |
| month by service / one day by team (same answers) | 55 / 11 ms | 26 / 13 ms |
| metadata after 32 commits | 99 files, 0.8 MB | ~1.1 MB of tables in PostgreSQL |

For whole-day loads the two are close (writing Parquet dominates); for small commits DuckLake is about 7× faster, because a commit is one database transaction instead of new manifest, manifest list and metadata.json files. Inlining is meant for tiny inserts: with 3,200-row batches it made commits slower and reads much slower. Iceberg's advantage is everything else: Spark, Trino, Flink, Snowflake, BigQuery and PyIceberg read it.

## What broke, and what I learned

1. **Writing to a branch that doesn't exist creates it empty**, without main's data (PyIceberg 0.12). The first branch spike showed 500 rows instead of 2,500. Create the branch from main's snapshot first.
2. **Per-partition overwrite refuses day partitioning** (`dynamic_partition_overwrite` needs identity transforms). Loads use an explicit filtered overwrite.
3. **The isolation level is the difference between a correct table and a duplicated day.** I expected optimistic concurrency to be enough on its own; under snapshot isolation two racing first loads both commit.
4. **PyIceberg's built-in retries run out under contention** (5 attempts), even for writers on different days that never conflict logically. The loader's own retry loop handles it, but more writers only add retries.
5. **Refused commits leave Parquet orphans**, so orphan cleanup is needed even without crashes, and it must have an age limit.
6. **Repartitioning by team made one-team queries read 6.5× fewer bytes but run twice as slow** in DuckDB. Reading the same files directly took 17 ms against 18 ms; the difference was in Iceberg metadata. DuckDB's time follows the files (manifest entries) a query has to consider, not bytes.
7. **Compaction made a query slower until the next append.** PyIceberg 0.12's file-level overwrite carries forward manifests holding only deleted entries; the next ordinary append drops them.
8. **My orphan cleaner kept 248 unreachable files** because it counted those deleted entries as references. A regression test now fails under the old rule (0 removed instead of 8).
9. **The incident-cost estimate was 15% low for a weekend incident**: the baseline mixed weekdays into a weekend comparison. Comparing the same hour on the same kind of day fixed it.
10. **The audit first read the staged day through a day filter**, so rows dated on another day would have slipped past the "inside the day" check while landing in that day's partition. It now reads exactly what the branch appended.

## Limits

- One machine, local disk: no object storage, so every metadata read is cheap here and would cost a network round trip on S3.
- PyIceberg writes everything; DuckDB only reads Iceberg here. No Spark, Trino or Flink.
- PyIceberg 0.12 can't write merge-on-read deletes, rewrite manifests or fast-forward branches (this project works around the last one).
- Crashes are process kills at chosen points plus random kills, not power loss.
- Catalog: PyIceberg's SQL catalog, not a REST catalog (Polaris, Lakekeeper, Glue).
- Timings come from one Windows laptop and vary between runs; the scripts print medians, and step 2 repeats each setting 3 times.

## Layout

| Path | What's in it |
|---|---|
| `lake/config.py` | catalog and DuckDB connections |
| `lake/gen.py` | the synthetic exports and incidents |
| `lake/tables.py` | schemas (explicit field IDs), partition spec, table properties |
| `lake/ingest.py` | replace-a-day loads, batch ids, conflict retries |
| `lake/meta.py`, `python -m lake show` | the metadata tree read by hand |
| `lake/faults.py`, `lake/worker.py` | crash points; a loader process for crash and race tests |
| `lake/evolution.py` | schema and partition changes, tags, rollback |
| `lake/maintenance.py` | compaction, expiry, orphan files |
| `lake/quality.py`, `lake/wap.py` | data-quality checks; write-audit-publish |
| `lake/analytics.py` | showback, spike detection, incident cost |
| `tests/` | 40 test functions, 47 cases |
| `scripts/` | one measurement script per step; `run_all.py` runs them in order |
| `results/stepN.txt` | the output of the last full run |
| `LEARN.md` | the concepts, questions to explain without notes, exercises |
