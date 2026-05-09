---
title: Quickstart
marimo-version: 0.23.4
---

# Quickstart

From zero to a 1M-row DuckLake table you can query, in under five minutes.

## 1. Install + start Postgres

```bash
git clone https://github.com/montanarograziano/ducklake_playground.git
cd ducklake_playground
just install
just up
```

Wait ~10 seconds for Postgres to become healthy. See [Installation](installation.md) for
prereqs and troubleshooting.

## 2. Open the marimo demo

```bash
just demo
# equivalent to: uv run marimo edit notebooks/streaming_demo.py
```

Marimo opens in your browser at <http://localhost:2718>.

## 3. Run all cells

Cells are reactive: change a parameter and downstream cells re-run automatically. The
default flow is:

| Cell | What it does |
|------|--------------|
| Imports | Loads `config.yaml`, prepares `DuckLakeEngine`, `StreamingGenerator`, etc. |
| Parameters | UI inputs: `row_count`, `table_name`, `storage_mode`, `chunk_size` |
| Attach | `engine.setup()` — installs DuckDB extensions, creates catalog DB, ATTACHes DuckLake |
| Write-time options | `CALL catalog.set_option(...)` — zstd compression, 16MB row groups |
| Stream-generate + write | `StreamingGenerator → arrow_reader → engine.write_overwrite()` |
| `DESCRIBE` | Native `mo.sql` cell — shows the schema |
| Sanity counts | `COUNT(*)`, `MIN/MAX(event_date)`, distinct partitions |
| Main query | A partition-pruned aggregation. Edit and re-run to iterate |
| `EXPLAIN ANALYZE` | Shows partition pruning + plan |
| Snapshots | DuckLake snapshot history |
| Maintenance | (informational) — compact, expire, cleanup commands |

## 4. Iterate on queries

The "Main query" cell is your sandbox. Edit the SQL — for example, replace the
aggregation with:

```sql
SELECT event_date, COUNT(*) FROM bench
WHERE int64_col > 0
GROUP BY event_date
ORDER BY event_date
```

…and the cell re-runs against the live engine. Wall time and RSS are not measured by
default in the SQL cell; for that, copy the query into a Python cell:

```python
with measure_time_and_memory() as t:
    result = con.execute(query).pl()
print(t[0])
```

## 5. Push to bigger scale

In the **Parameters** cell, change `row_count` to `100_000_000`. Watch:

- Wall time: ~5-10 minutes on a typical laptop.
- Peak RSS: a few hundred MB delta over baseline (the engine streams; no full
  materialization in Python).
- Disk usage: ~25 GB.

If memory climbs higher than expected, lower `chunk_size` to 100_000. See
[Tuning](tuning.md) for the full story.

## 6. Persistence

The DuckLake table persists across notebook restarts in:

- Postgres: catalog metadata (database `ducklake_playground_local`).
- Local filesystem: data files under `data/ducklake/event_date=YYYY-MM-DD/*.parquet`.

Re-running the write cell with `mode="overwrite"` replaces the data; with `mode="append"`
it accumulates. To wipe everything:

```bash
just down-clean
```

This stops containers and deletes all data — fresh start.

## What to read next

- [Configuration](configuration.md) — every YAML knob, explained
- [Architecture](architecture.md) — how the pieces interact
- [Notebooks](notebooks.md) — full walkthrough of both notebooks
- [Tuning](tuning.md) — bound memory at 100M+ rows
- [Maintenance and Time Travel](maintenance.md) — snapshots, compaction, rollback
