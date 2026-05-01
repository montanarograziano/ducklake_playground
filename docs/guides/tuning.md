# Tuning

Three layers of knobs, in order of impact:

1. **`chunk_size`** (generator) — bounds Python-side memory.
2. **`target_file_size_mb` / `parquet_row_group_size_bytes`** (writer) — bounds writer
   memory and controls file/row-group sizing.
3. **`partition cardinality`** (`event_date` range) — implicitly determined by the
   number of distinct partition values; affects fan-out memory.

## chunk_size (generator)

The size of each `pa.RecordBatch` yielded by `StreamingGenerator`. Set in
`config.yaml`'s `playground.batch_size`, or pass `chunk_size=...` to `GeneratorSpec`
directly.

| chunk_size | Per-batch Arrow memory | Total chunks for 100M rows | Per-chunk Python overhead |
|-----------:|-----------------------:|---------------------------:|--------------------------:|
| 10_000 | ~2.5 MB | 10,000 | significant (~25s aggregate) |
| 100_000 | ~25 MB | 1,000 | ~3s |
| **250_000** | **~60 MB** | **400** | **~1s — recommended** |
| 1_000_000 | ~250 MB | 100 | ~0.3s |
| 5_000_000 | ~1.25 GB | 20 | negligible, but big mem |

The sweet spot at this schema (~250 bytes/row) is **roughly the size of one Parquet
output file** in uncompressed Arrow. With `target_file_size_mb=16` and ~3-4× compression,
that's 50-60 MB Arrow ≈ 200-250K rows.

### Why not bigger

- 1M rows = 250 MB Arrow per chunk. With DuckDB's parallel writer (multi-threaded under
  `preserve_insertion_order=false`), several chunks can be in flight simultaneously,
  multiplying memory.
- 5M rows = 1.25 GB Arrow per chunk. Materialization in Python before handoff is wasteful
  given DuckDB chunks the batch internally anyway.

### Why not smaller

- 10K rows = each chunk crosses the Python/Rust boundary in ~2-3 ms regardless of size.
  At 10,000 chunks that's ~30 seconds of pure overhead.
- DuckDB's vectorized pipeline likes batches ≥ ~10K rows.

## target_file_size + row groups (writer)

These control what DuckDB's Parquet writer does **per partition**.

### DuckLake (writer-side, persistent)

Set on the catalog via `set_option`. These persist in Postgres metadata across
sessions:

```sql
CALL playground_ducklake_local.set_option('parquet_version', 2);
CALL playground_ducklake_local.set_option('parquet_compression', 'zstd');
CALL playground_ducklake_local.set_option('parquet_row_group_size_bytes', '16MB');
```

| Option | Effect |
|--------|--------|
| `parquet_version` | `2` enables `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` — ~10-30% better compression than v1 |
| `parquet_compression` | `zstd` typically gives ~2× better compression than snappy at similar speed |
| `parquet_row_group_size_bytes` | Compressed bytes per row group. Smaller = more granular pruning, more metadata. Larger = better compression, coarser pruning |

A row group setting that's **larger than the file size** has effectively no effect:
the file flushes (because the partition switches) before the row group fills. To
make the row-group setting meaningful, keep it ≤ the typical file size.

### Recommended: align row group with target file

For most demos, set them to the same byte target:

```sql
CALL catalog.set_option('parquet_row_group_size_bytes', '16MB');
```

Combined with sorted-by-partition input from `StreamingGenerator`, files end up
with **one row group per file** at ~16 MB compressed. Clean, predictable.

## Partition cardinality

Implicit knob — controlled by `GeneratorSpec.date_start` / `date_end`. Default is
30 days = 30 partitions. To stress higher cardinality, override programmatically:

```python
import datetime as dt
spec = GeneratorSpec(
    schema_config=cfg.schema,
    total_rows=10_000_000,
    chunk_size=cfg.batch_size,
    date_start=dt.date(2024, 1, 1),
    date_end=dt.date(2024, 12, 31),  # 365 partitions
)
```

Memory cost scales linearly with partition count **if** writes are not partition-sorted.
With our sorted streams, only 1-2 partitions are open at any moment regardless of
cardinality, so the multiplier disappears.

## Validation procedure

Three numbers worth measuring at scale:

1. **Generator memory** — should equal `chunk_size × ~250 bytes`, not more.
2. **Writer Δ RSS** — should be bounded by `~target_file_size × small_constant`,
   not scale linearly with total rows.
3. **Process peak RSS** — climbs as files are written and mmap'd, plateaus near the
   total written bytes (file cache, reclaimable).

```python
from ducklake_playground import (
    DuckLakeEngine, GeneratorSpec, StreamingGenerator,
    load_config, measure_time_and_memory,
)

cfg = load_config("config.yaml")
engine = DuckLakeEngine()
engine.setup(cfg, "local")

for n in [1_000_000, 10_000_000, 100_000_000]:
    gen = StreamingGenerator(GeneratorSpec(
        schema_config=cfg.schema, total_rows=n,
        chunk_size=cfg.batch_size, seed=cfg.schema.seed,
    ))
    with measure_time_and_memory() as t:
        engine.write_overwrite("scaling_test", gen.arrow_reader(), gen.schema)
    timing = t[0]
    print(
        f"n={n:>10,}  wall={timing.wall_time_seconds:6.2f}s  "
        f"peak={timing.peak_rss_mb:6.0f}MB  delta={timing.delta_rss_mb:6.0f}MB"
    )

engine.teardown("scaling_test")
engine.close()
```

The `delta_rss_mb` column should grow sub-linearly with `n` (roughly constant or with
log-like growth). If it grows linearly, something is buffering the entire dataset
internally — diagnose with smaller chunks, smaller `target_file_size`, or fewer
partitions.

## Trade-offs at a glance

| Choice | Memory | Disk | Read perf |
|--------|:------:|:----:|:---------:|
| Smaller `chunk_size` | ↓ | — | — |
| Smaller `target_file_size` | ↓ | ↑ files | ↓ (more metadata, more seeks) |
| Smaller row groups | — | ↑ metadata | ↑ for selective filters, ↓ for full scans |
| ZSTD vs snappy | — | ↓ ~2× | ↓ slight CPU |
| More partitions | ↑ (writer) | ↑ small files | ↑ pruning |

Defaults in `config.yaml` are chosen to bound memory at 100M rows on 16 GB without
sacrificing wall-time obviously.
