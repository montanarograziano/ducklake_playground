"""DuckLake streaming demo (marimo).

Generate synthetic data with a parametrized row count, write it to DuckLake via streaming,
then query it interactively with native marimo SQL cells.

Open with: ``uv run marimo edit notebooks/streaming_demo.py`` (or ``just demo``).
"""

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports + load config."""
    import sys
    from pathlib import Path

    import marimo as mo

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root / "src") not in sys.path:
        sys.path.insert(0, str(repo_root / "src"))

    from ducklake_playground import (
        DuckLakeEngine,
        GeneratorSpec,
        StreamingGenerator,
        load_config,
        measure_time_and_memory,
    )

    config = load_config(repo_root / "config.yaml")
    return (
        DuckLakeEngine,
        GeneratorSpec,
        StreamingGenerator,
        config,
        measure_time_and_memory,
        mo,
    )


@app.cell
def _(config, mo):
    """Parameters. Re-run downstream cells after editing these."""
    row_count = mo.ui.number(
        value=1_000_000, start=1_000, step=10_000, label="Row count"
    )
    table_name = mo.ui.text(value="demo_table", label="Table name")
    storage_mode = mo.ui.dropdown(
        options=config.storage_modes,
        value=config.default_storage_mode,
        label="Storage mode",
    )
    chunk_size = mo.ui.number(
        value=config.batch_size, start=10_000, step=10_000, label="Chunk size"
    )
    mo.vstack([row_count, table_name, storage_mode, chunk_size])
    return chunk_size, row_count, storage_mode, table_name


@app.cell
def _(DuckLakeEngine, config, mo, storage_mode):
    """Attach to the DuckLake catalog. Idempotent across reruns."""
    engine = DuckLakeEngine()
    engine.setup(config, storage_mode.value)
    con = engine.connection
    catalog = engine.catalog_name
    mo.md(
        f"Attached **{catalog}** &nbsp;|&nbsp; storage=`{storage_mode.value}` "
        f"&nbsp;|&nbsp; data_path=`{engine.data_path}`"
    )
    return catalog, con, engine


@app.cell
def _(catalog, table_name):
    """Fully qualified table name used by all SQL cells."""
    fq = f"{catalog}.main.{table_name.value}"
    return (fq,)


@app.cell
def _(catalog, con, mo):
    """Write-time DuckLake catalog options.

    These persist in the Postgres catalog, so they apply to all subsequent writes
    against this catalog. Setting them here (BEFORE the write) ensures the streaming
    write below uses zstd compression and matched row-group sizing.
    """
    _ = mo.sql(
        f"""
        CALL {catalog}.set_option('parquet_version', 2);
        CALL {catalog}.set_option('parquet_compression', 'zstd');
        CALL {catalog}.set_option('parquet_row_group_size_bytes', '16MB');
        """,
        engine=con,
    )
    return


@app.cell
def _(
    GeneratorSpec,
    StreamingGenerator,
    chunk_size,
    config,
    engine,
    measure_time_and_memory,
    mo,
    row_count,
    table_name,
):
    """Stream-generate + write. Memory bounded by ``chunk_size``."""
    gen = StreamingGenerator(
        GeneratorSpec(
            schema_config=config.schema,
            total_rows=int(row_count.value),
            chunk_size=int(chunk_size.value),
            seed=config.schema.seed,
        )
    )
    with measure_time_and_memory() as _t:
        engine.write_overwrite(table_name.value, gen.arrow_reader(), gen.schema)
    timing = _t[0]
    mo.md(
        f"Wrote **{int(row_count.value):,}** rows in **{timing.wall_time_seconds:.2f}s** "
        f"&nbsp;|&nbsp; peak RSS **{timing.peak_rss_mb:.0f} MB** "
        f"(delta **{timing.delta_rss_mb:.0f} MB**)"
    )
    return


@app.cell
def _(con, fq, mo):
    """Schema preview."""
    _ = mo.sql(f"DESCRIBE {fq}", engine=con)
    return


@app.cell
def _(con, fq, mo):
    """Sanity row count + partition span."""
    _ = mo.sql(
        f"""
        SELECT COUNT(*)         AS n,
               MIN(event_date)  AS min_date,
               MAX(event_date)  AS max_date,
               COUNT(DISTINCT event_date) AS distinct_partitions
        FROM {fq}
        """,
        engine=con,
    )
    return


@app.cell
def _(con, fq, mo):
    """Main demo query — partition-pruned aggregation. Edit and re-run."""
    _ = mo.sql(
        f"""
        SELECT varchar_col,
               COUNT(*)            AS cnt,
               SUM(int64_col)      AS sum_val,
               AVG(float64_col)    AS avg_val,
               MIN(event_date)     AS min_date,
               MAX(event_date)     AS max_date
        FROM {fq}
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'
        GROUP BY varchar_col
        ORDER BY cnt DESC
        LIMIT 20
        """,
        engine=con,
    )
    return


@app.cell
def _(con, fq, mo):
    """EXPLAIN ANALYZE — useful on stage to show partition pruning."""
    _ = mo.sql(
        f"""
        EXPLAIN ANALYZE
        SELECT varchar_col, COUNT(*) AS cnt, AVG(float64_col) AS avg_val
        FROM {fq}
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-20'
        GROUP BY varchar_col
        """,
        engine=con,
    )
    return


@app.cell
def _(catalog, con, mo):
    """Snapshot history (DuckLake time-travel surface)."""
    _ = mo.sql(
        f"SELECT * FROM {catalog}.snapshots() ORDER BY snapshot_id DESC LIMIT 10",
        engine=con,
    )
    return


@app.cell
def _(catalog, con, mo):
    """Maintenance — run after several writes to compact small files.

    Uncomment to execute. Order matters: merge first, then expire snapshots, then cleanup.
    """
    _ = mo.md(
        """
        ```sql
        CALL ducklake_merge_adjacent_files('{catalog}');
        CALL ducklake_expire_snapshots('{catalog}', older_than => INTERVAL '7 days');
        CALL ducklake_cleanup_old_files('{catalog}', cleanup_all => true);
        ```
        """.replace("{catalog}", catalog)
    )
    return


@app.cell
def _(engine):
    """Close the engine on notebook shutdown."""
    import atexit

    atexit.register(engine.close)
    return


if __name__ == "__main__":
    app.run()
