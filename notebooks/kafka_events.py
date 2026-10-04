"""Inspect the Kafka-fed DuckLake table while the consumer runs separately.

Open with ``just kafka-marimo`` after ``just up-kafka`` and a consumer batch.
"""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="full")


@app.cell
def _():
    import sys
    from pathlib import Path

    import marimo as mo

    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root / "src") not in sys.path:
        sys.path.insert(0, str(repo_root / "src"))

    from ducklake_playground import DuckLakeEngine, load_config

    config = load_config(repo_root / "config.yaml")
    return DuckLakeEngine, config, mo


@app.cell
def _(mo):
    mo.md("""
    # Kafka events in DuckLake

    Run `just up-kafka`, `just consume-events`, then
    `just produce-events --count 12 --duplicate-every 4` in separate terminals.
    The table appears after the consumer flushes its first batch (by default,
    after 50 messages or 5 seconds). Rerun the query cells to see later batches:
    writes from the external consumer do not trigger marimo reactivity.
    """)
    return


@app.cell
def _(DuckLakeEngine, config, mo):
    engine = DuckLakeEngine()
    engine.setup(config, "local")
    con = engine.connection
    catalog = engine.catalog_name
    table = engine.qualified_table("kafka_events")
    mo.md(f"Connected to `{table}`. Data path: `{engine.data_path}`")
    return catalog, con, table


@app.cell
def _(con, mo, table):
    _ = mo.sql(
        f"""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT id) AS distinct_ids
        FROM {table}
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo, table):
    _ = mo.sql(
        f"""
        SELECT id, timestamp_col, event_date, varchar_col, int64_col
        FROM {table}
        ORDER BY timestamp_col DESC
        LIMIT 20
        """,
        engine=con
    )
    return


@app.cell
def _(catalog, con, mo):
    _ = mo.sql(
        f"""
        SELECT snapshot_id, snapshot_time
        FROM {catalog}.snapshots()
        ORDER BY snapshot_id DESC
        LIMIT 10
        """,
        engine=con
    )
    return


if __name__ == "__main__":
    app.run()
