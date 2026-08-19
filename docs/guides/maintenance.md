---
title: Maintenance
marimo-version: 0.23.4
---

# Maintenance and Time Travel

DuckLake's snapshot model unlocks two capabilities you'll want to demo:

1. **Compaction** — merge small adjacent files after several writes.
2. **Time travel and rollback** — query historical snapshots; revert to a previous one.

## Compaction

DuckLake provides three procedures that work together:

```sql
-- 1. Compact small adjacent files into larger ones (per partition).
CALL ducklake_merge_adjacent_files('playground_ducklake_local');

-- 2. Mark old snapshots as expired (frees the right to delete their files).
CALL ducklake_expire_snapshots(
    'playground_ducklake_local',
    older_than => INTERVAL '7 days'
);

-- 3. Delete orphaned data files (files no live snapshot references).
CALL ducklake_cleanup_old_files(
    'playground_ducklake_local',
    cleanup_all => true
);
```

Order matters: merge → expire → cleanup. Each is idempotent.

Dropping a table removes its catalog entry only. It intentionally does **not** delete the
shared `DATA_PATH`, because another table may use it; use the retention-aware procedures
above to remove unreferenced files.

| Procedure | What it does | When to call |
|-----------|--------------|--------------|
| `merge_adjacent_files` | Rewrites small files into larger ones, partition-aware. Creates a new snapshot containing the consolidated file set | After many writes / after merge_upsert (which produces lots of small post-merge files) |
| `expire_snapshots` | Marks snapshots older than `older_than` as expired. Snapshots stay in the catalog until a `cleanup_old_files` call removes their data | When you want to free disk |
| `cleanup_old_files` | Deletes data files that no live snapshot references. **Not** reversible | After expiring snapshots |

### When to skip compaction

For a **single streaming write** (the demo's default), compaction is mostly a no-op:
the writer already produces files of `target_file_size` ≈ 16 MB. Run it only if you've
done multiple writes or an `merge_upsert`.

### Observing the effect

Before:
```sql
SELECT COUNT(*) AS files,
       SUM(file_size_bytes) / 1024 / 1024 AS total_mb,
       AVG(file_size_bytes) / 1024 / 1024 AS avg_mb
FROM ducklake_table_files('playground_ducklake_local.main.demo_table');
```

After `CALL ducklake_merge_adjacent_files(...)`:
- File count drops, average file size grows.
- A new snapshot is registered (visible in `snapshots()`).

## Time travel (read-only)

Two equivalent forms:

```sql
-- 1. Per-query: AT (VERSION => N) or AT (TIMESTAMP => ...)
SELECT * FROM playground_ducklake_local.main.demo_table AT (VERSION => 3);
SELECT * FROM playground_ducklake_local.main.demo_table AT (TIMESTAMP => now() - INTERVAL '1 hour');

-- 2. Per-attach: pin an ATTACH to a snapshot
ATTACH 'ducklake:postgres:dbname=ducklake_playground_local host=localhost user=user password=password'
   AS old_view (SNAPSHOT_VERSION 3, READ_ONLY);

SELECT * FROM old_view.main.demo_table;
```

Per-query is convenient for one-offs. Per-attach gives you a stable handle that other
SQL can reference, including in joins:

```sql
SELECT current.id, current.value, old.value AS old_value
FROM playground_ducklake_local.main.demo_table current
JOIN old_view.main.demo_table old USING (id)
WHERE current.value <> old.value;
```

### Listing snapshots

```sql
SELECT * FROM playground_ducklake_local.snapshots()
ORDER BY snapshot_id DESC LIMIT 10;
```

Each row has `snapshot_id`, `snapshot_time`, `schema_version`, and a `changes` summary.

## Rollback

DuckLake (current stable) has **no dedicated `rollback_to_snapshot` procedure** like
Iceberg. Roll back by re-writing the table from a time-travel SELECT:

```sql
-- Make snapshot 3 the new current state
CREATE OR REPLACE TABLE playground_ducklake_local.main.demo_table AS
SELECT * FROM playground_ducklake_local.main.demo_table AT (VERSION => 3);
```

This produces a **new snapshot** (e.g. snapshot 7) whose contents equal snapshot 3.
Snapshots 4-6 remain in the catalog as history. From this point, reads without
`AT (VERSION => ...)` see snapshot 3's data.

Trade-offs:

- **Cost:** rewrites all data files (the `CREATE OR REPLACE ... AS SELECT` materializes).
  At 100M rows × ~25 GB on disk, that's a 25 GB read + write. Iceberg's
  metadata-only rollback would be instant; DuckLake doesn't currently offer that fast path.
- **Lineage:** the rollback is itself an explicit snapshot, so audit trails are preserved.
- **Idempotent:** rerunning produces identical content but a new snapshot id.

If you only need to **read** the old data (not change the head), prefer the per-query
or per-attach time-travel forms — they're free.

## Caveats

### Don't run cleanup if you might want to roll back

After:
```sql
CALL ducklake_expire_snapshots('catalog', older_than => INTERVAL '1 day');
CALL ducklake_cleanup_old_files('catalog', cleanup_all => true);
```

…the data files of expired snapshots are gone. Time travel to those snapshots fails
silently (you'll get a runtime error when reading), and the rewrite-rollback trick
can't restore them.

For demos, either:

- Skip `cleanup_old_files` entirely.
- Pass a longer `older_than` (e.g. `INTERVAL '30 days'`) so recent snapshots stay
  reclaimable.

### `set_option` is catalog-scoped

DuckLake's `CALL catalog.set_option(...)` writes the setting to the Postgres catalog.
Subsequent writes from any process (including the CLI) pick up the new setting. If
you run multiple demos against the same catalog with different options, write-side
behavior changes silently. Be explicit about resetting options in `setup()` if you
care about reproducibility.

### Snapshot history grows unbounded without expiry

Every write produces a new snapshot. Without periodic `expire_snapshots`, the catalog
metadata grows linearly with write count — small in absolute terms (a few KB per
snapshot) but unbounded.

## A demo-friendly maintenance cell

Add this near the end of your notebook, commented out by default:

```python
import marimo as mo

@app.cell
def _(catalog, con, mo):
    \"\"\"Maintenance — uncomment to compact files after many writes.\"\"\"
    _ = mo.sql(
        f\"\"\"
        CALL ducklake_merge_adjacent_files('{catalog}');
        CALL ducklake_expire_snapshots('{catalog}', older_than => INTERVAL '7 days');
        CALL ducklake_cleanup_old_files('{catalog}', cleanup_all => true);
        \"\"\",
        engine=con,
    )
    return
```

For Jupyter, use `con.execute(...)` instead of `mo.sql(..., engine=con)`.
