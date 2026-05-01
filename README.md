# DuckLake Playground

Streaming data generation + a thin DuckLake engine + an interactive marimo notebook for
ad-hoc DuckLake demos. Built around three load-bearing ideas:

1. **Memory-bounded data generation.** A `StreamingGenerator` yields `pa.RecordBatch` chunks
   of configurable size; total dataset size is independent of process RSS. Generate 100
   million rows on a 16 GB laptop without OOM.
2. **Streaming writes.** The DuckLake engine consumes a `pa.RecordBatchReader` directly via
   `INSERT INTO ... SELECT * FROM <reader>`. One DuckLake snapshot per call, no full
   materialization in Python.
3. **Partition-sorted output.** Rows are emitted in `event_date`-ascending order so the
   Parquet writer only ever has 1-2 partition files open at once. This is what makes
   bounded memory possible at high partition counts.

## Quick start

```bash
# 1. Install
just install

# 2. (Optional) override default credentials
cp .env.example .env       # edit if you want non-default Postgres/MinIO creds
                           # remember to mirror changes in config.yaml

# 3. Start Postgres (DuckLake catalog backend)
just up

# 4. Open the marimo demo notebook
just demo
```

In the notebook:

* Set `row_count` (e.g. `100_000_000`) and `chunk_size` (e.g. `250_000`).
* Run all cells. Watch the streaming write happen with `event_date` partitioning.
* Iterate on the `mo.sql(...)` query cells — full table scans, partition-pruned filters,
  EXPLAIN ANALYZE.

## Repo layout

```
ducklake_playground/
├── src/ducklake_playground/
│   ├── config.py            # YAML loader + dataclasses
│   ├── data_generator.py    # StreamingGenerator
│   ├── engine.py            # DuckLakeEngine
│   └── metrics.py           # psutil-backed timing + RSS sampler
├── notebooks/
│   ├── streaming_demo.py    # marimo (primary)
│   └── streaming_demo.ipynb # Jupyter (mirror)
├── docs/                    # Zensical site
├── tests/                   # pytest smoke tests
├── config.yaml              # all knobs
├── docker-compose.yml       # Postgres (and optional MinIO)
└── justfile                 # task runner
```

## Documentation

```bash
just preview-docs   # http://localhost:8000
just build-docs     # static site under site/
```

The docs cover installation, configuration, architecture, the data generator, the engine,
the notebooks, tuning (`chunk_size`, `target_file_size`, row groups), and DuckLake
maintenance (compaction, snapshot expiry, time-travel, rollback).

## Requirements

* Python 3.12+
* uv (package manager)
* Docker (for the Postgres catalog)
* DuckDB 1.5.2+ (installed automatically via `uv sync`)

## License

MIT.
