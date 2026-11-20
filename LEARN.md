# Learning notes: lakehouse

## 1. What a lakehouse is, and what a table format adds

A data lake is files (Parquet) in storage. Cheap and open, but "a table" is just a directory: no transactions, no safe concurrent writers, no schema changes without rewriting, and a reader listing the directory mid-write sees half a load.

A **table format** (Apache Iceberg, Delta Lake, Hudi, DuckLake) adds a metadata layer that says exactly which files make up the table at each moment. That gives you warehouse behaviour on lake files: atomic commits, snapshots and time travel, schema evolution, and many engines reading the same table (here PyIceberg writes and DuckDB reads).

## 2. Iceberg's metadata tree

```
catalog (a row in PostgreSQL)  ->  metadata.json   schemas, partition specs, snapshots, refs (branches, tags)
                                     -> manifest list (Avro, one per snapshot)   one row per manifest + partition summaries
                                        -> manifest (Avro)   one row per data file + per-column stats (min, max, nulls)
                                           -> data file (Parquet)
```
- **A commit** writes new files bottom-up, then swaps the catalog pointer from the old metadata.json to the new one with a compare-and-swap (`UPDATE ... SET metadata_location = new WHERE metadata_location = old`). Nothing is visible until the swap; the swap is atomic.
- **Planning** reads the tree top-down and skips manifests (by partition summaries) and files (by column stats) that can't match the filter. Then only the needed Parquet files are read.
- Read it yourself: `python -m lake show`, or `lake/meta.py` (JSON with the standard library, Avro with DuckDB).

## 3. Commits under failure and concurrency

- **Crashes** (step 2): die after writing data files or after writing metadata.json, and the table is unchanged; the files left behind are **orphans**. Die after the swap but before hearing back, and the commit stands; a rerun must notice (the batch id in the snapshot summary) and skip.
- **Optimistic concurrency:** two writers start from the same snapshot; the second swap fails; the writer re-reads the table and **re-validates** before retrying. PyIceberg 0.12 retries 4 times by default, then gives up.
- **Isolation levels:**
  - *serializable* (the default): a replace-by-filter fails if anyone added files matching the filter meanwhile.
  - *snapshot*: it doesn't check that; two racing first loads of the same day both commit and the day is there **twice** (shown in a test).
- **One table, one commit at a time.** More writers add retries, not throughput: commits per second stayed flat from 1 to 8 writers. Batch commits, or have one committer (what Flink's Iceberg sink does).
- **Orphan cleanup needs an age limit.** A running writer's files look exactly like orphans until it commits. Delete them with no age limit and the table points at missing files (shown in a test). Spark's procedure defaults to 3 days.

## 4. Evolution without rewriting

- **Columns are tracked by ID, not name.** Rename, drop and re-add are metadata commits; old files are read through the ID mapping. Read the same files by name (DuckDB `read_parquet`) and a rename hides half the values while a re-added column resurrects dropped data (both shown in tests).
- **Only safe type changes:** int -> long, float -> double, decimal precision up. Narrowing or string -> number is refused.
- **Hidden partitioning:** the partition is a transform of a real column (`day(usage_start)`). Queries filter on `usage_start`; nobody writes `WHERE dt = ...`.
- **Partition evolution:** a new spec applies to new files; old files keep their spec; planning handles both. Rewriting old data into the new spec is a separate, optional job.
- **Time travel, tags, rollback:** every snapshot stays readable until it expires. A tag pins one (the month-end close); rollback moves main back (milliseconds, metadata only).

## 5. Keeping a table healthy

- **Small files** come from frequent small commits or over-fine partitioning. Each file costs an open and footer read, and each commit adds a manifest; planning and queries slow down.
- **Compaction** rewrites them. It must be a *file-level replace* ("remove exactly these files, add these"), committed as an *overwrite*:
  - not "replace whatever matches this filter": that could delete a file appended meanwhile;
  - not delete + append: incremental readers would see compacted rows as new.
  If a file it rewrote was replaced meanwhile (a restatement), the commit must fail, or old rows come back.
- **Sorting during compaction** clusters values, so Parquet min/max stats let readers skip row groups: pruning without extra partitions.
- **Row-level deletes:** copy-on-write rewrites every file containing a deleted row (huge amplification for a few rows); merge-on-read writes small delete files instead and makes reads merge them. PyIceberg 0.12 only does copy-on-write.
- **Expiry** drops old snapshots (except tagged ones and branch heads); then orphan cleanup deletes the files only they used. Time travel to an expired snapshot is gone.

## 6. Write-audit-publish

Stage each load on a branch, run the checks there, and fast-forward main only if they pass. Readers of main never see a bad load, and publishing is one commit. Fast-forward is only safe if main hasn't moved since the branch was made; otherwise it would silently drop the other commit. The checks that catch real failures:
- **control totals** from the export's manifest (rows, sum) catch truncated or doubled transfers;
- **uniqueness, validity and reference checks** catch bad rows;
- an **anomaly gate** against recent history catches the bug where the provider's own totals agree with the bad data.

## 7. When DuckLake instead of Iceberg

DuckLake keeps the metadata in SQL tables (here PostgreSQL) instead of files. A commit is a database transaction, small inserts can be stored inline in the catalog, and there are no manifests to read. Iceberg is the interoperable standard (Spark, Trino, Flink, Snowflake, BigQuery, DuckDB, PyIceberg); DuckLake is newer and read mainly by DuckDB. Step 6 measures both on the same workload.

## 8. What broke while building

1. Writing to a branch that didn't exist yet created it **empty** (no main data). Create branches from main's snapshot first.
2. Snapshot isolation let two racing first loads of a day both commit (the day twice). Serializable refused the second.
3. PyIceberg's own retries (5 attempts) ran out with 8 writers on *different* days; the loader's outer retry handles it.
4. A refused commit left its Parquet files behind: orphans without any crash.
5. Partitioning by team cut bytes read 6.5× but DuckDB got slower; the time was in decoding manifest entries, not reading data.
6. Compaction made a query slower until the next append: PyIceberg carries manifests that only list deleted files.
7. My orphan cleaner counted those deleted entries as references and kept 248 dead files. A regression test now fails under the old rule.
8. The incident-cost estimate was 15% low on a weekend incident: the baseline compared weekend hours with weekday hours.
9. The audit first read the staged data through a day filter, so rows dated on another day could slip past the "inside the day" check.

## 9. Explain it without notes

1. What exactly happens, in order, when an Iceberg commit runs? Where is the atomic step?
2. A writer dies after writing its Parquet files. What does the table look like, and what cleans up?
3. A writer's commit landed but it never heard back. How does a rerun avoid loading twice?
4. Two writers replace the same day at once. What happens under serializable isolation, and under snapshot isolation?
5. Why doesn't adding writers increase commit throughput on one table?
6. Why must orphan-file cleanup have an age limit?
7. Why is a rename safe in Iceberg and dangerous for plain Parquet read by name?
8. What is hidden partitioning, and what happens to old files when the partition spec changes?
9. How would you reproduce the numbers reported at month-end close after late credits arrived?
10. Why must compaction be a file-level replace committed as an overwrite?
11. What does copy-on-write cost for a one-row delete, and what is the alternative?
12. Walk through write-audit-publish, and say when publishing must be refused.

## 10. Your turn

1. **Merge-on-read deletes:** write position delete files (or Iceberg v3 deletion vectors) with an engine that supports it (Spark or DuckDB), and measure write vs read cost against copy-on-write.
2. **A REST catalog:** run Apache Polaris or Lakekeeper locally and point PyIceberg and DuckDB's `ATTACH` at it instead of the SQL catalog.
3. **Spark or Trino** reading the same tables: a third engine checking the same snapshots.
4. **Streaming ingestion:** a single committer taking batches from a queue every minute; measure the file count and compaction schedule it needs.
5. **Object storage:** MinIO instead of the local disk; measure what each metadata read costs over the network.
