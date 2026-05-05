---
title: Configuration
marimo-version: 0.23.4
---

# Configuration

Everything is in `config.yaml` at the repo root. Loaded by
[`load_config()`](../reference/index.md#configuration) into immutable dataclasses.

## Section: `playground`

Top-level metadata and write-side knobs.

```yaml
playground:
  name: "DuckLake Playground"
  batch_size: 250_000
  target_file_size_mb: 16
  parquet_row_group_size: 122_880
```

| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `name` | str | "DuckLake Playground" | Display name; surfaced in the notebook header |
| `batch_size` | int | 250_000 | Rows per generator chunk. Bounds Python-side memory. See [Tuning](tuning.md) |
| `target_file_size_mb` | int | 16 | Hint passed to engines that accept it (Delta uses it; DuckLake uses `set_option` instead) |
| `parquet_row_group_size` | int | 122_880 | Row group size in **rows** for Parquet writers that take a row count |

## Section: `storage_modes`

Which storage backends are available. Set the active default with `default_storage_mode`.

```yaml
storage_modes:
  - local
  # - s3                 # uncomment to enable S3/MinIO

default_storage_mode: local
```

The notebook's storage selector is populated from this list. To enable S3, uncomment the
line and run `just up-s3` to start MinIO.

## Section: `schema`

Defines the synthetic table schema. The generator always injects two columns automatically:

- `id_col` (configurable name, `int64`, NOT NULL): sequential row id.
- `event_date` (always named `event_date`, `pa.date32`, NOT NULL): partition column,
  derived from `id_col` so rows are emitted in partition-sorted order.

The `columns` list adds further columns for type coverage.

```yaml
schema:
  id_col: "id"
  seed: 42
  merge_overlap_ratio: 0.10
  columns:
    - { name: "int64_col", type: "int64" }
    - { name: "varchar_col", type: "varchar", cardinality: 1000 }
    # ...
```

| Key | Type | Notes |
|-----|------|-------|
| `id_col` | str | Name of the auto-injected sequential id column |
| `seed` | int | Base RNG seed (per-chunk seeds derived via `SeedSequence.spawn`) |
| `merge_overlap_ratio` | float | For `iter_merge_batches`: fraction (0..1) of rows that reuse base IDs (UPDATE) vs. new IDs (INSERT) |
| `columns` | list | User-defined columns — see [Data Generation](data-generation.md) for type reference |

### Supported column types

| Type | Required keys | Notes |
|------|---------------|-------|
| `int8` / `int16` / `int32` / `int64` | — | Random over the full type range |
| `float32` / `float64` | — | Random in `[-1e6, 1e6]` / `[-1e15, 1e15]` |
| `decimal` | `precision`, `scale` | Default precision=18, scale=4 |
| `date` | — | Random in 2024-01-01..2028-12-30 |
| `datetime` / `timestamp` | — | Microsecond precision, 2020-2025 range, `timestamp` is UTC |
| `varchar` | `cardinality` | Dictionary-encoded; pool of `value_000`..`value_{N-1}` |
| `text` | `avg_length` | `large_string`; normal-distributed lengths |
| `boolean` | — | Uniform 0/1 |
| `list` | `child_type` (always `int32`), `avg_length` | List of int32 |
| `struct` | `fields` | List of `"name:type"` strings; types: `int32`, `varchar`, `float64` |
| `map` | `key_type` (always `varchar`), `value_type` (always `int32`), `avg_length` | Unique keys per row (DuckDB constraint) |

## Section: `filter`

Default predicate used by the engine's `read_filtered_scan` helper and by the demo
notebook's main query.

```yaml
filter:
  date_range: ["2024-01-10", "2024-01-20"]
  varchar_values: ["value_001", "value_002", "value_003"]
```

`date_range` applies to `event_date` (partition pruning). `varchar_values` applies via
`varchar_col IN (...)`. The default range covers ~1/3 of the partition span, exercising
pruning without making it trivial.

## Section: `postgres`

DuckLake metadata catalog connection. Must point at a running Postgres.

```yaml
postgres:
  host: "localhost"
  port: 5432
  database: "ducklake_playground"
  user: "user"
  password: "password"
```

The engine creates a separate database **per storage mode** (e.g. `ducklake_playground_local`
and `ducklake_playground_s3`) so the metadata never conflicts when switching modes.

## Section: `s3`

S3-compatible storage config. Used only when `storage_mode == "s3"`.

```yaml
s3:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "miniopassword"
  bucket: "warehouse"
  ducklake_prefix: "ducklake/"
```

For real AWS S3, set `endpoint` to `https://s3.<region>.amazonaws.com` and use real
credentials.

!!! warning "Do not commit real credentials"
    Treat `config.yaml` as committed config. For real credentials, use environment
    variables and substitute via your shell or a `.env` loader.

## Section: `local`

Local filesystem storage config. Used when `storage_mode == "local"`.

```yaml
local:
  base_path: "data/"
  ducklake_prefix: "ducklake/"
```

`base_path` is resolved against **the YAML file's directory** (not the current working
directory). This makes the data location stable across notebook entry points (Jupyter
sets cwd to the notebook dir; CLI runs from repo root; both end at the same absolute path).

The full DuckLake data path is therefore `{yaml_dir}/{base_path}/{ducklake_prefix}` —
under defaults: `<repo>/data/ducklake/`.

## Programmatic loading

```python
from ducklake_playground import load_config

config = load_config("config.yaml")
print(config.name)
print(config.schema.columns)
print(config.local.base_path)  # absolute, resolved
```

The `PlaygroundConfig` dataclass is frozen — to override values, edit the YAML and reload.