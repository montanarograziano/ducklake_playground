---
title: Data Generation
marimo-version: 0.23.4
---

# Data Generation

## The schema

Every generated table has the same shape:

```
id          int64       NOT NULL    sequential row id
event_date  date32      NOT NULL    partition column, derived from id
<user columns from config.yaml>
```

The generator always injects the first two columns. Everything else comes from
`schema.columns` in `config.yaml`. See [Configuration: Schema](configuration.md#section-schema)
for the full type reference.

## Why event_date is derived from id

A single deterministic function:

```python
n_partitions = (date_end - date_start).days + 1
rows_per_partition = ceil(total_rows / n_partitions)
partition_idx = id // rows_per_partition  # same for primary + merge streams
event_date = date_start + partition_idx
```

Consequences:

1. **Rows arrive in `event_date`-ascending order.** This is the key property that keeps
   the Parquet writer bounded — only 1-2 partitions open at any time.
2. **Update rows in `iter_merge_batches` land in the same partition as the original row.**
   Because the partition function depends only on `id`, an UPDATE for `id=5` always
   writes to the same partition where it originally lived.
3. **Reproducibility.** Same `seed` + same `total_rows` ⇒ identical bytes on disk.

## The streaming contract

Each streaming adapter call produces a fresh deterministic stream. Individual
`pa.RecordBatchReader` values remain single-pass, so make a new reader when reusing the
generator.

```python
gen = StreamingGenerator(GeneratorSpec(...))
reader = gen.arrow_reader()
engine.write_overwrite("t", reader, gen.schema)

# DON'T:
# engine.write_append("t2", reader, gen.schema)  # reader is exhausted!

# DO:
engine.write_append("t2", gen.arrow_reader(), gen.schema)
```

## RNG strategy

The base seed comes from `config.schema.seed` (default 42). For chunked generation,
per-chunk seeds are derived via:

```python
sub_seeds = np.random.SeedSequence(seed).spawn(n_chunks)
```

`SeedSequence.spawn` produces statistically independent child streams. This is
better than `seed + chunk_idx` (which produces correlated streams) and is the
NumPy-recommended pattern for parallel/chunked work.

The merge stream uses `seed + 1` for ID sampling and `seed + 2` for per-chunk data
generation, so primary and merge streams are independent.

## Vectorized columns

Most column types use NumPy vectorized generation — no Python `for` loops over rows.
Specifically:

- **Numeric / boolean / date / timestamp:** single `np.random` call per chunk.
- **Decimal:** `np.random.uniform` + cast.
- **Varchar:** dictionary-encoded via `pa.DictionaryArray.from_arrays`. Both DuckLake
  and Delta-compatible writers honor the dictionary on disk for Parquet `RLE_DICTIONARY`
  encoding.
- **Text (`large_string`):** built from raw byte buffers (`pa.Array.from_buffers`),
  skipping any Python list step. Random lengths are normal-distributed.
- **List (`int32`):** offsets + flat values arrays, both numpy-allocated.

A few types still have per-row work:

- **Map:** per-row sampling without replacement (DuckDB MAP requires unique keys per row).
  Cost is bounded by `avg_length` (default 3), so even at 1M rows the loop runs ~3M
  iterations — small relative to total write time.
- **Struct with varchar fields:** per-row index into a name pool.

## Schema stability

Each emitted batch is `cast()` to the canonical schema:

```python
batch = pa.RecordBatch.from_arrays(arrays, names=names)
return batch.cast(self.schema)
```

This catches subtle bugs at scale — e.g. if one chunk produced `int32` for a column
declared `int64`, downstream consumers (DuckDB, delta-rs) would fail with a confusing
mid-stream error. The `cast()` fails immediately and loudly.

## Partition cardinality

`GeneratorSpec.date_start` and `date_end` define the partition range. Default is
**30 days** (2024-01-01..2024-01-30), giving 30 partitions regardless of `total_rows`.

Why bounded:

- At 10K rows: ~330 rows/partition (small files but writable).
- At 100M rows: ~3.3M rows/partition (healthy multi-MB Parquet files).
- At 730 partitions (one year of daily granularity), 10K rows yields 14 rows/partition,
  forcing the writer to keep 730 buffers open simultaneously and OOMing on chained ops.
  We learned this the hard way; 30 is conservative.

To stress high-cardinality partitioning, override `date_start`/`date_end` in code:

```python
import datetime as dt
spec = GeneratorSpec(
    schema_config=cfg.schema,
    total_rows=10_000_000,
    chunk_size=cfg.batch_size,
    seed=42,
    date_start=dt.date(2024, 1, 1),
    date_end=dt.date(2024, 12, 31),  # 365 partitions
)
```

…and expect proportionally higher writer memory.

## Merge data

`iter_merge_batches(overlap_ratio)` produces an Arrow stream of upsert sources:

- `update_count = int(total_rows * overlap_ratio)` rows reuse existing IDs sampled
  uniformly from `[0, total_rows)` without replacement.
- `insert_count = total_rows - update_count` rows have new IDs in `[total_rows, 2*total_rows)`.
- All IDs are concatenated and **sorted ascending** so the stream is partition-sorted.
- For each chunk, `event_date` is recomputed from the actual chunk IDs (not chunk
  position) so update rows land in the same partition as their base row.

Use it:

```python
gen = StreamingGenerator(GeneratorSpec(..., total_rows=1_000_000))
engine.write_overwrite("t", gen.arrow_reader(), gen.schema)

merge_gen = StreamingGenerator(GeneratorSpec(..., total_rows=1_000_000))
engine.merge_upsert("t", merge_gen.merge_arrow_reader(0.10), merge_key="id")
```

This produces ~100K UPDATE rows + ~900K INSERT rows.

## Verifying determinism

```python
gen1 = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=1000, seed=42))
gen2 = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=1000, seed=42))

t1 = pa.Table.from_batches(list(gen1.iter_batches()))
t2 = pa.Table.from_batches(list(gen2.iter_batches()))

assert t1.equals(t2)  # passes
```

Different seeds produce different (but stable) data.
