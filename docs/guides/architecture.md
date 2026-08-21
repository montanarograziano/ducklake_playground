---
title: Architecture
marimo-version: 0.23.4
---

# Architecture

Three components, one data flow.

## Components

```
┌────────────────────────────────────────────────────────────────────┐
│                              Notebook                               │
│   (marimo: streaming_demo.py  /  Jupyter: streaming_demo.ipynb)     │
└────────────────────────────────────────────────────────────────────┘
                  ↓                                  ↓
┌──────────────────────────────┐   ┌──────────────────────────────┐
│      StreamingGenerator      │   │       DuckLakeEngine          │
│   src/.../data_generator.py  │   │   src/.../engine.py            │
│                              │   │                                │
│  - GeneratorSpec (frozen)    │   │  - setup(config, mode)         │
│  - iter_batches() →          │   │  - write_overwrite/append      │
│    pa.RecordBatch            │   │  - merge_upsert                │
│  - arrow_reader() →          │   │  - read_full_scan/aggregation  │
│    pa.RecordBatchReader      │──→│  - get_disk_usage              │
│  - iter_merge_batches()      │   │  - connection (DuckDB)         │
└──────────────────────────────┘   └──────────────────────────────┘
                                                  ↓
                            ┌─────────────────────────────────────┐
                            │       DuckDB + ducklake ext         │
                            │   INSERT INTO catalog.main.t        │
                            │      SELECT * FROM <reader>         │
                            └─────────────────────────────────────┘
                              ↓                          ↓
                ┌─────────────────────┐    ┌────────────────────────┐
                │  Postgres (catalog) │    │  Local FS or MinIO     │
                │  ducklake_*_{mode}  │    │  Parquet, partitioned  │
                │                     │    │  by event_date         │
                └─────────────────────┘    └────────────────────────┘
```

## Data flow (a single `write_overwrite` call)

1. **Notebook** instantiates `StreamingGenerator(GeneratorSpec(...))`. No data has
   been generated yet.
2. **Notebook** calls `gen.arrow_reader()`. This returns a `pa.RecordBatchReader` that
   wraps `iter_batches()`. Still lazy.
3. **Notebook** calls `engine.write_overwrite(table_name, reader, schema)`.
4. **Engine** drops any existing table, creates a fresh one with the schema, and
   registers the reader with DuckDB as `_arrow_src`.
5. **Engine** issues `INSERT INTO catalog.main.table SELECT * FROM _arrow_src`.
6. **DuckDB** pulls one record batch at a time from the reader. Each batch flows
   through DuckDB's Arrow scan operator into the DuckLake writer.
7. **DuckLake writer** routes rows to per-partition Parquet writers based on
   `event_date`. Because the stream is partition-sorted (see [Data Generation](data-generation.md)),
   only 1-2 partition writers are open at any time.
8. **Catalog metadata** for the new files is committed to Postgres as a single new
   snapshot.
9. **Reader** is exhausted; engine unregisters `_arrow_src`. One DuckLake snapshot.

This is what makes the headline claim work: at any moment, the in-memory data is bounded
by *one chunk* + *one or two partition write buffers*, regardless of total rows.

## Why each piece exists

### StreamingGenerator

A standalone, engine-agnostic data source. Knows nothing about DuckLake. Produces
deterministic Arrow record batches with:

- A canonical `event_date` partition column derived from row id.
- User-defined columns from `SchemaConfig`.
- Schema-stable across batches (each batch is `cast()` to the canonical schema).
- Bounded RNG: `np.random.SeedSequence(seed).spawn(n)` per chunk.

### DuckLakeEngine

The DuckLake-specific glue. Owns the DuckDB connection. Provides three layers:

- **Lifecycle:** `setup`, `teardown`, `close`.
- **Operations:** `write_*`, `read_*`, `merge_upsert`.
- **Introspection:** `get_disk_usage`, `get_postgres_metadata_size`,
  `connection`/`catalog_name`/`data_path` properties.

The connection is exposed for ad-hoc SQL because the engine deliberately doesn't
wrap every query — that's what notebooks are for.

### Notebook

Where you actually demo. The marimo version uses native `mo.sql(...)` cells with
`engine=engine.connection` so SQL has syntax highlighting and DataFrame results,
while the streaming write stays in Python (because it needs a `RecordBatchReader`).

## Catalog isolation by storage mode

The engine constructs a catalog name and Postgres database name from the storage mode:

| Storage mode | Catalog name | Postgres DB |
|--------------|--------------|-------------|
| `local` | `playground_ducklake_local` | `ducklake_playground_local` |
| `s3` | `playground_ducklake_s3` | `ducklake_playground_s3` |

This means:

- Switching modes never causes metadata conflicts (DuckLake stores `DATA_PATH` in the
  catalog; one catalog per data path).
- You can have local and S3 demos coexisting on the same Postgres instance.
- Tearing down one mode doesn't affect the other.

The Postgres database is auto-created on first `setup()` call via `psycopg`.

## Path resolution

`config.yaml`'s `local.base_path` is resolved against the **YAML file's directory**, not
the process's current working directory. This is the difference between

- launching a Jupyter notebook (cwd = `notebooks/`),
- running a Python script from the repo root, and
- using marimo with VS Code's notebook extension

…all producing the same absolute path. Without this rule, the same `config.yaml` can
produce conflicting `DATA_PATH` values across launchers, which DuckLake's catalog
rejects with an explicit error.

## Memory budget at a glance

For 100M rows on a 16 GB host with default config:

| Component | Working set |
| ----------- | ------------- |
| Generator (one chunk, 250K rows × ~250 bytes) | ~60 MB |
| DuckDB execution pipeline | ~50-200 MB |
| Parquet writer (1-2 active partitions, 16 MB target) | ~50-100 MB |
| Process baseline (Python + extensions) | ~3 GB |
| OS file cache for written data (mmap'd, reclaimable) | up to 25 GB |
| **Allocated heap delta** | **~150-300 MB** |

RSS (what `psutil` reports) climbs as files are written and mmap'd, but the actually
allocated heap stays bounded. See [Tuning](tuning.md) for how to verify.
