---
title: Notebooks
marimo-version: 0.23.4
---

# Notebooks

Two notebooks ship in `notebooks/`:

| File | Format | Primary use |
|------|--------|-------------|
| `streaming_demo.py` | marimo | **Live demos** — reactive cells, native SQL syntax highlighting |
| `streaming_demo.ipynb` | Jupyter | VS Code, JupyterLab, or anything that speaks `.ipynb` |

Both share the same data flow and produce the same DuckLake catalog. Pick whichever
matches your environment.

## marimo: `streaming_demo.py`

```bash
just demo
# or: uv run marimo edit notebooks/streaming_demo.py
```

Opens at <http://localhost:2718>. Reactive: change the `row_count` slider and the write
cell re-executes; edit the SQL in the query cell and the result re-renders.

### Cells

1. **Imports + load_config** — Python. Sets up `sys.path`, loads `config.yaml`.
2. **Parameters** — Python (UI). `row_count`, `table_name`, `storage_mode`, `chunk_size`
   as `mo.ui` widgets.
3. **Attach to DuckLake** — Python. `engine.setup(config, storage_mode.value)` returns
   `con`, `catalog`, `engine`.
4. **Fully qualified table name** — Python.
5. **Write-time options** — `mo.sql(...)` — `CALL catalog.set_option(...)` for parquet
   version, compression (zstd), row group size.
6. **Stream-generate + write** — Python. `StreamingGenerator(...).arrow_reader()` →
   `engine.write_overwrite(...)`, wrapped in `measure_time_and_memory`.
7. **`DESCRIBE`** — `mo.sql(...)`. Schema preview.
8. **Sanity counts** — `mo.sql(...)`. Row count, partition span, distinct partitions.
9. **Main query** — `mo.sql(...)`. Partition-pruned aggregation. **This is where you
   iterate during the demo** — edit and re-run.
10. **EXPLAIN ANALYZE** — `mo.sql(...)`. Shows partition pruning in the plan.
11. **Snapshots** — `mo.sql(...)`. DuckLake snapshot history.
12. **Maintenance** — Markdown only. Reference SQL for compaction; uncomment to run.
13. **Cleanup hook** — Python. `atexit.register(engine.close)`.

### How `mo.sql(..., engine=con)` works

Marimo's native SQL cells default to a private DuckDB connection. By passing
`engine=engine.connection` (the DuckDB connection that already has DuckLake
attached), every `mo.sql` cell runs against the same catalog the engine wrote to.
No registration tricks, no hidden state.

Result: the SQL cell renders as syntax-highlighted SQL in the marimo UI, executes
against your DuckLake catalog, and outputs a DataFrame inline.

### Editing SQL cells in the UI

Marimo lets you toggle a cell between Python and SQL via the editor. The `.py`
on-disk representation is always Python (with `mo.sql(...)` calls); the UI is the
nice presentation layer.

Python f-strings are used to interpolate `fq` (the fully qualified table name).
This is mildly less polished in the UI than a static SQL cell, but it's necessary
because the catalog name depends on `storage_mode` chosen at runtime.

## Jupyter: `streaming_demo.ipynb`

```bash
just jupyter
# or: uv run jupyter lab notebooks/streaming_demo.ipynb
```

Same flow, but:

- Parameters are plain Python variables (`ROW_COUNT = 1_000_000`) instead of UI sliders.
- SQL is run via `con.execute(query).pl()` and the resulting Polars DataFrame is the
  cell's last expression (auto-rendered).
- Maintenance commands are commented out by default; uncomment to run.

VS Code's notebook extension handles `.ipynb` files; just open the file and select
the project's Python interpreter (`.venv/bin/python`) when prompted.

## Switching between them

Both notebooks write to the same DuckLake catalog (`playground_ducklake_local`).
Tables and snapshots produced by one are visible to the other. To clean up between
runs, drop the table:

```sql
DROP TABLE playground_ducklake_local.main.demo_table;
```

…or wipe everything via `just down-clean`.

## When to use which

- **marimo** for live demos, audience-facing presentations, exploratory iteration on
  query SQL with reactive re-execution.
- **Jupyter** for sequential workflows, notebooks that need to be reviewed in PRs as
  rendered `.ipynb`, or environments where marimo isn't installed.