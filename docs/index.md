---
title: Index
marimo-version: 0.23.4
---

# DuckLake Playground

A focused, single-purpose toolkit for **demoing DuckLake interactively** without the
boilerplate. Three load-bearing pieces:

1. **[`StreamingGenerator`](reference/index.md#data-generator)** — emits Arrow record
   batches of configurable size with a fixed schema (id, partition column, plus user
   columns for type coverage). Memory bounded by `chunk_size`, independent of total rows.
2. **[`DuckLakeEngine`](reference/index.md#engine)** — thin wrapper around DuckDB's
   `ducklake` extension. Streams writes from a `pa.RecordBatchReader` into DuckLake;
   exposes the underlying DuckDB connection for ad-hoc SQL.
3. **[Notebooks](guides/notebooks.md)** — a marimo notebook (primary, with native
   `mo.sql` cells) and a Jupyter mirror. Generate data, run queries, see wall time and
   peak RSS inline.

## What this is for

Standing on stage in front of an audience and writing 100 million rows to DuckLake
on a 16 GB laptop without an OOM, then running EXPLAIN ANALYZE on a partition-pruned
query. Repeatable. Boring. Reproducible from `config.yaml`.

## What this is not

Not a benchmark suite (use the parent `poor-man-lakehouse` repo for that). Not a
production data layer. Not a library you pip-install — it's a playground repo you clone.

## How it works

```
+---------------------------------------------------------------+
|                    notebooks/streaming_demo.py                 |
|                              (marimo)                          |
+---------------------------------------------------------------+
|   StreamingGenerator    ────────►   pa.RecordBatchReader       |
|   (event_date-sorted)               (zero-copy stream)         |
+--------------------------------------------┬------------------+
                                             ▼
+---------------------------------------------------------------+
|   DuckLakeEngine.write_overwrite(reader, schema)               |
|     ↓ register reader as Arrow view                            |
|     ↓ INSERT INTO catalog.main.table SELECT * FROM <view>      |
+--------------------------------------------┬------------------+
                                             ▼
+--------------------------+    +-------------------------------+
|  Postgres (catalog)      |◄──►|  Local FS or MinIO (Parquet)  |
|  ducklake_playground_*   |    |  partitioned by event_date     |
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
