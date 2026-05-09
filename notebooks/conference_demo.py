"""DuckLake conference live demo (marimo).

Assumes the table already exists — run ``streaming_demo.py`` first to populate it with
millions of rows. This notebook is designed to be walked through on stage in ~15 minutes,
covering: connect + explore, ACID transactions, time travel, schema evolution + CDC,
MERGE/upsert, and maintenance.

Open with: ``uv run marimo edit notebooks/conference_demo.py``
"""

import marimo

__generated_with = "0.23.5"
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

    from ducklake_playground import DuckLakeEngine, load_config

    config = load_config(repo_root / "config.yaml")
    storage_mode = mo.ui.dropdown(
        options=config.storage_modes,
        value=config.default_storage_mode,
        label="Storage mode",
    )
    storage_mode
    return DuckLakeEngine, config, mo, storage_mode


@app.cell
def _(DuckLakeEngine, config, mo, storage_mode):
    """Attach to the existing DuckLake catalog. Table must already exist."""

    engine = DuckLakeEngine()
    engine.setup(config, storage_mode.value)
    con = engine.connection
    catalog = engine.catalog_name
    TABLE = "demo_table"
    fq = f"{catalog}.main.{TABLE}"

    mo.md(
        f"**Connected** to `{catalog}` "
        f"| storage = `{config.default_storage_mode}` "
        f"| data path = `{engine.data_path}`"
    )
    print(fq)
    return TABLE, catalog, con, fq


@app.cell
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        DESCRIBE {fq}
        """,
        engine=con
    )
    return


@app.cell
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        SELECT COUNT(*)                  AS total_rows,
               MIN(event_date)           AS first_date,
               MAX(event_date)           AS last_date,
               COUNT(DISTINCT event_date) AS partitions
        FROM {fq}
        """,
        engine=con
    )
    return


@app.cell
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        SELECT varchar_col,
               COUNT(*)        AS cnt,
               SUM(int64_col)  AS total,
               AVG(float64_col) AS avg_val
        FROM {fq}
        WHERE event_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-15'
        GROUP BY varchar_col
        ORDER BY cnt DESC
        LIMIT 10
        """,
        engine=con
    )
    return


@app.cell
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        EXPLAIN ANALYZE
        SELECT varchar_col, COUNT(*) AS cnt
        FROM {fq}
        WHERE event_date = DATE '2024-01-15'
        GROUP BY varchar_col
        """,
        engine=con
    )
    return


@app.cell
def _(catalog, con, fq, mo):
    """Record the current row count and snapshot version before the transaction."""

    pre_count = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    pre_snapshot = con.execute(
        f"SELECT MAX(snapshot_id) FROM {catalog}.snapshots()"
    ).fetchone()[0]
    mo.md(f"**Before transaction:** {pre_count:,} rows | snapshot v{pre_snapshot}")
    return pre_count, pre_snapshot


@app.cell
def _(catalog, con, fq, mo, pre_count, pre_snapshot):
    """Atomic multi-statement transaction: INSERT + UPDATE in one snapshot.

    If anything fails, nothing is written. Both statements land in a single
    DuckLake snapshot (one new version in the catalog).
    """

    con.execute("BEGIN TRANSACTION")
    try:
        # Insert two new rows
        con.execute(
            f"""
            INSERT INTO {fq} (id, event_date, int64_col, float64_col, varchar_col)
            VALUES
                (900000001, DATE '2024-01-15', 1499, 99.95, 'value_042'),
                (900000002, DATE '2024-01-15', 49,   19.99, 'value_007')
            """
        )
        # Update one of them (10% discount)
        con.execute(
            f"""
            UPDATE {fq}
            SET float64_col = float64_col * 0.9
            WHERE id = 900000001
            """
        )
        con.execute("COMMIT")
        tx_status = "COMMITTED"
    except Exception as exc:
        con.execute("ROLLBACK")
        tx_status = f"ROLLED BACK: {exc}"

    post_count = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
    post_snapshot = con.execute(
        f"SELECT MAX(snapshot_id) FROM {catalog}.snapshots()"
    ).fetchone()[0]
    mo.md(
        f"**Transaction {tx_status}**\n\n"
        f"- Before: {pre_count:,} rows (v{pre_snapshot})\n"
        f"- After: {post_count:,} rows (v{post_snapshot}, +{post_count - pre_count})\n"
        f"- Both INSERT and UPDATE landed atomically in one DuckLake snapshot"
    )
    return (post_snapshot,)


@app.cell
def _(con, fq, mo):
    """Verify the transaction results."""

    _ = mo.sql(
        f"""
        SELECT id, event_date, int64_col, float64_col, varchar_col
        FROM {fq}
        WHERE id IN (900000001, 900000002)
        ORDER BY id
        """,
        engine=con,
    )
    return


@app.cell
def _(catalog, con, mo):
    """List all snapshots — each write creates a new version."""

    _ = mo.sql(
        f"""
        SELECT *
        FROM {catalog}.snapshots()
        ORDER BY snapshot_id DESC
        LIMIT 10
        """,
        engine=con,
    )
    return


@app.cell
def _(mo, post_snapshot, pre_snapshot):
    """Show the two snapshot versions we will compare.

    ``pre_snapshot`` was captured before the transaction, ``post_snapshot`` after.
    Using explicit IDs avoids the "table does not exist at version X" error that
    occurs when the second-to-last catalog snapshot predates the table.
    """

    mo.md(
        f"**Before-tx snapshot:** v{pre_snapshot} | **After-tx snapshot:** v{post_snapshot}"
    )
    return


@app.cell
def _(con, fq, mo, pre_snapshot):
    """Time travel: query the table AS OF the previous snapshot.

    This reads the table state BEFORE our transaction, without any rollback.
    """

    _ = mo.sql(
        f"""
        SELECT COUNT(*) AS row_count_before_tx
        FROM {fq} AT (VERSION => {pre_snapshot})
        """,
        engine=con,
    )
    return


@app.cell
def _(con, fq, mo, pre_snapshot):
    """Prove the inserted rows did not exist in the previous version."""

    _ = mo.sql(
        f"""
        SELECT id, event_date, float64_col
        FROM {fq} AT (VERSION => {pre_snapshot})
        WHERE id IN (900000001, 900000002)
        """,
        engine=con,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    Change data feed: what changed between the two snapshots?

    Returns INSERT, UPDATE_BEFORE, UPDATE_AFTER rows with change_type column.
    This is DuckLake's built-in CDC — no Kafka or external tooling required.
    """)
    return


@app.cell
def _(TABLE, catalog, con, mo, post_snapshot, pre_snapshot):
    _df = mo.sql(
        f"""
        SELECT *
        FROM {catalog}.table_changes('{TABLE}', {pre_snapshot}, {post_snapshot})
        ORDER BY change_type, id
        LIMIT 20
        """,
        engine=con
    )
    return


@app.cell
def _(con, fq, mo):
    """Add a new column — no Parquet rewrite needed.

    DuckLake stores the schema change in the catalog metadata only. Existing
    Parquet files are untouched; the new column reads as NULL for old rows.
    """

    try:
        con.execute(
            f"ALTER TABLE {fq} ADD COLUMN priority VARCHAR DEFAULT 'normal'"
        )
        mo.md(
            "**Column `priority` added.** Existing Parquet files untouched (metadata-only change)."
        )
    except Exception as exc:
        mo.md(f"Column may already exist: {exc}")
    return


@app.cell
def _(con, fq, mo):
    """Verify: new column appears in schema, old rows have the default."""

    _ = mo.sql(
        f"""
        SELECT id, event_date, varchar_col, priority
        FROM {fq}
        WHERE id IN (900000001, 900000002, 1, 2, 3)
        ORDER BY id
        LIMIT 5
        """,
        engine=con,
    )
    return


@app.cell
def _(con, fq, mo):
    """Rename a column — again, metadata-only. No file rewrite."""

    try:
        con.execute(f"ALTER TABLE {fq} RENAME COLUMN priority TO urgency")
        mo.md("**Renamed `priority` to `urgency`.** Zero Parquet I/O.")
    except Exception as exc:
        mo.md(f"Rename note: {exc}")
    return


@app.cell
def _(con, fq, mo):
    """Drop the column to leave the table clean for the next demo."""

    try:
        con.execute(f"ALTER TABLE {fq} DROP COLUMN urgency")
        mo.md("**Dropped `urgency`.** Table schema restored to original.")
    except Exception as exc:
        mo.md(f"Drop note: {exc}")
    return


@app.cell
def _(con, fq, mo):
    """MERGE INTO: upsert pattern. Update existing rows + insert new ones.

    - id 900000001 exists: UPDATE its int64_col
    - id 999999999 is new: INSERT it
    Both happen atomically in one DuckLake snapshot.
    """

    con.execute(
        f"""
        MERGE INTO {fq} AS target
        USING (
            VALUES
                (900000001, DATE '2024-01-15', CAST(9999 AS BIGINT),
                 CAST(42.0 AS DOUBLE), 'value_042'),
                (999999999, DATE '2024-01-20', CAST(7777 AS BIGINT),
                 CAST(55.5 AS DOUBLE), 'value_001')
        ) AS source(id, event_date, int64_col, float64_col, varchar_col)
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET int64_col = source.int64_col,
                       float64_col = source.float64_col
        WHEN NOT MATCHED THEN
            INSERT (id, event_date, int64_col, float64_col, varchar_col)
            VALUES (source.id, source.event_date, source.int64_col,
                    source.float64_col, source.varchar_col)
        """
    )
    mo.md("**MERGE complete.** id=900000001 updated, id=999999999 inserted.")
    return


@app.cell
def _(con, fq, mo):
    """Verify the MERGE results."""

    _ = mo.sql(
        f"""
        SELECT id, event_date, int64_col, float64_col, varchar_col
        FROM {fq}
        WHERE id IN (900000001, 999999999)
        ORDER BY id
        """,
        engine=con,
    )
    return


@app.cell
def _(TABLE, catalog, con, mo):
    """Show file statistics before compaction."""

    _ = mo.sql(
        f"""
        SELECT COUNT(*)                                    AS total_files,
               ROUND(SUM(data_file_size_bytes) / 1e6, 2)  AS total_mb,
               ROUND(AVG(data_file_size_bytes) / 1e6, 2)  AS avg_file_mb,
               ROUND(MIN(data_file_size_bytes) / 1e6, 2)  AS min_file_mb,
               ROUND(MAX(data_file_size_bytes) / 1e6, 2)  AS max_file_mb
        FROM ducklake_list_files('{catalog}', '{TABLE}')
        """,
        engine=con,
    )
    return


@app.cell
def _(catalog, con, mo):
    """Compact small files — DuckLake's equivalent of OPTIMIZE / compaction.

    Merges adjacent small Parquet files into larger ones for better scan perf.
    Run this after many small writes (streaming, CDC, frequent upserts).
    """

    con.execute(f"CALL ducklake_merge_adjacent_files('{catalog}')")
    mo.md("**`ducklake_merge_adjacent_files` complete.** Small files merged.")
    return


@app.cell
def _(TABLE, catalog, con, mo):
    """File statistics after compaction — fewer, larger files."""

    _ = mo.sql(
        f"""
        SELECT COUNT(*)                                    AS total_files,
               ROUND(SUM(data_file_size_bytes) / 1e6, 2)  AS total_mb,
               ROUND(AVG(data_file_size_bytes) / 1e6, 2)  AS avg_file_mb
        FROM ducklake_list_files('{catalog}', '{TABLE}')
        """,
        engine=con,
    )
    return


@app.cell
def _(catalog, con, mo):
    """Expire old snapshots — reclaim catalog space.

    Keeps only snapshots newer than the threshold. Expired snapshots can no
    longer be used for time travel.
    """

    # Expire snapshots older than 1 day (aggressive for demo purposes).
    # `older_than` expects a TIMESTAMP, so compute it from `now() - INTERVAL`.
    con.execute(
        f"CALL ducklake_expire_snapshots('{catalog}', older_than => now() - INTERVAL '1 day')"
    )
    mo.md(
        "**`ducklake_expire_snapshots` complete.** Old versions pruned from catalog."
    )
    return


@app.cell
def _(catalog, con, mo):
    """Clean up orphaned data files left behind by expired snapshots."""

    con.execute(
        f"CALL ducklake_cleanup_old_files('{catalog}', cleanup_all => true)"
    )
    mo.md(
        "**`ducklake_cleanup_old_files` complete.** Unreferenced Parquet files deleted."
    )
    return


@app.cell
def _(catalog, con, mo):
    """Final snapshot list — show the pruned history."""

    _ = mo.sql(
        f"""
        SELECT snapshot_id, snapshot_time, changes
        FROM {catalog}.snapshots()
        ORDER BY snapshot_id DESC
        LIMIT 10
        """,
        engine=con,
    )
    return


@app.cell
def _(con, fq, mo):
    _df = mo.sql(
        f"""
        DELETE FROM {fq} WHERE id IN (900000001, 900000002, 999999999);
        """,
        engine=con
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
