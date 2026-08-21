---
title: Index
marimo-version: 0.23.4
---

# DuckLake Playground

A self-contained sandbox for exploring [DuckLake](https://ducklake.select/) end-to-end:
streaming ingestion, partitioned writes, ACID transactions, time travel, schema evolution,
CDC, and table maintenance. Clone it, bring up Postgres and (optionally) MinIO with
Docker, and you have a working lakehouse on your laptop.

## What this repo gives you

- **A configurable streaming generator** ([`StreamingGenerator`](reference/index.md#data-generator))
  that emits Arrow record batches with a fixed schema (an auto-injected `id` and
  `event_date` partition column plus configurable user columns for type coverage).
  Memory is bounded by `chunk_size` — you can generate 100M rows on a 16 GB laptop.
- **A thin DuckLake engine** ([`DuckLakeEngine`](reference/index.md#engine)) built on
  DuckDB's `ducklake` extension. Streams writes from a `pa.RecordBatchReader` into
  DuckLake, exposes the underlying DuckDB connection for ad-hoc SQL, and namespaces
  the catalog per storage backend so `local` and `s3` runs coexist on the same
  Postgres instance.
- **Two interchangeable storage backends** wired through `config.yaml`:
  - **Local filesystem** — Parquet under `data/ducklake/`, no extra services.
  - **S3-compatible** — MinIO container included. The current secret setup is
    MinIO-specific; real AWS S3 needs TLS/region credential handling in the engine.
- **PostgreSQL as the DuckLake catalog**, started via the bundled
  `docker-compose.yml`. Metadata (snapshots, schema versions, file listings) lives
  here.
- **Two demo notebooks** in `notebooks/`:
  - `streaming_demo` — generate data, stream it into DuckLake, inspect snapshots,
    iterate on SQL with marimo's reactive cells (or the Jupyter mirror).
  - `conference_demo` — walks through ACID transactions, time travel, CDC
    (`table_changes`), schema evolution (`ALTER TABLE` without rewrites), MERGE/upsert,
    and maintenance (`merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files`).

## What you can do with it

After cloning and running `just install && just up`, you can:

- Write millions of partitioned rows to DuckLake in a single streaming snapshot.
- Run partition-pruned queries with `EXPLAIN ANALYZE` to inspect file selection.
- Snapshot every write, query historical versions with `AT (VERSION => N)`, and diff
  two snapshots with `table_changes(...)`.
- Add, rename, and drop columns without rewriting any Parquet files (metadata-only
  schema evolution).
- Run MERGE upserts and compaction (`ducklake_merge_adjacent_files`).
- Switch between local-FS and S3/MinIO storage by changing a single line in the
  notebook — catalogs and Postgres databases are kept separate per backend.
- Tune `chunk_size`, target file size, and Parquet row group size and observe the
  effect on memory, wall time, and resulting file layout.

## How it works

```
+---------------------------------------------------------------+
|                    notebooks/streaming_demo                    |
|              (marimo .py and Jupyter .ipynb)                   |
+---------------------------------------------------------------+
|   StreamingGenerator    --------->   pa.RecordBatchReader      |
|   (event_date-sorted)                (zero-copy stream)        |
+--------------------------------------------+------------------+
                                             v
+---------------------------------------------------------------+
|   DuckLakeEngine.write_overwrite(reader, schema)               |
|     -> register reader as Arrow view                           |
|     -> INSERT INTO catalog.main.table SELECT * FROM <view>     |
+--------------------------------------------+------------------+
                                             v
+--------------------------+    +-------------------------------+
|  Postgres (catalog)      |<-->|  Local FS or MinIO (Parquet)  |
|  ducklake_playground_*   |    |  partitioned by event_date    |
+--------------------------+    +-------------------------------+
```

## Quick links

- [Installation](guides/installation.md) — uv, Docker, prereqs
- [Quickstart](guides/quickstart.md) — five minutes to first query
- [Configuration](guides/configuration.md) — every knob in `config.yaml`, explained
- [Architecture](guides/architecture.md) — how the pieces fit
- [Data Generation](guides/data-generation.md) — schema, RNG, partitioning, types
- [Notebooks](guides/notebooks.md) — marimo + Jupyter walkthroughs
- [Tuning](guides/tuning.md) — `chunk_size`, `target_file_size`, row groups
- [Maintenance and Time Travel](guides/maintenance.md) — compaction, snapshots, rollback
- [API Reference](reference/index.md) — auto-generated from docstrings
