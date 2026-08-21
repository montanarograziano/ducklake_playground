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
| ----- | ------ | --------- | --------- |
| `name` | str | "DuckLake Playground" | Display name; surfaced in the notebook header |
| `batch_size` | int | 250_000 | Rows per generator chunk. Bounds Python-side memory. See [Tuning](tuning.md) |
| `target_file_size_mb` | int | 16 | Recommended value to mirror in the notebook's DuckLake `parquet_row_group_size_bytes` option |
| `parquet_row_group_size` | int | 122_880 | Informational row-count target for experiments; DuckLake's active setting is the byte-based notebook option |

## Choosing the storage backend

The storage backend (`local` or `s3`) is **not** in `config.yaml`. It's selected
directly in the notebook by setting a top-level constant before calling
`engine.setup(config, storage_mode)`:

```python
# notebooks/streaming_demo.py (marimo) or streaming_demo.ipynb (Jupyter)
STORAGE_MODE = "local"  # or "s3"
```

The engine derives a separate catalog name and Postgres database per mode
(`playground_ducklake_local` vs `playground_ducklake_s3`), so the two never
conflict — see [Architecture › Catalog isolation by storage mode](architecture.md).

To use S3 you need a running MinIO (or real S3 endpoint): `just up-s3`, then edit
the `s3:` section below.

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
| ----- | ------ | ------- |
| `id_col` | str | Name of the auto-injected sequential id column |
| `seed` | int | Base RNG seed (per-chunk seeds derived via `SeedSequence.spawn`) |
| `merge_overlap_ratio` | float | For `iter_merge_batches`: fraction (0..1) of rows that reuse base IDs (UPDATE) vs. new IDs (INSERT) |
| `columns` | list | User-defined columns — see [Data Generation](data-generation.md) for type reference |

### Supported column types

| Type | Required keys | Notes |
| ------ | --------------- | ------- |
| `int8` / `int16` / `int32` / `int64` | — | Random over the full type range |
| `float32` / `float64` | — | Random in `[-1e6, 1e6]` / `[-1e15, 1e15]` |
| `decimal` | `precision`, `scale` | Default precision=18, scale=4 |
| `date` | — | Random over a 5-year span: 2024-01-01..2028-12-30. This is the standalone `date` column for type coverage, **not** the `event_date` partition column |
| `datetime` / `timestamp` | — | Microsecond precision, 2020-01-01..2024-12-31 range, `timestamp` is UTC |
| `varchar` | `cardinality` | Dictionary-encoded; pool of `value_000`..`value_{N-1}` |
| `text` | `avg_length` | `large_string`; normal-distributed lengths |
| `boolean` | — | Uniform 0/1 |
| `list` | `child_type` (always `int32`), `avg_length` | List of int32 |
| `struct` | `fields` | List of `"name:type"` strings; types: `int32`, `varchar`, `float64` |
| `map` | `key_type` (always `varchar`), `value_type` (always `int32`), `avg_length` | Unique keys per row (DuckDB constraint) |

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

S3-compatible storage config. Used only when the notebook sets `STORAGE_MODE = "s3"`.

```yaml
s3:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "miniopassword"
  bucket: "warehouse"
  ducklake_prefix: "ducklake/"
```

The current engine secret is intentionally MinIO-specific (`URL_STYLE 'path'` and
`USE_SSL false`). Real AWS S3 needs a small engine/config change for region, TLS, and
credential-provider handling; changing only this endpoint is not sufficient.

!!! warning "Do not commit real credentials"
    Treat `config.yaml` as committed config. For real credentials, use environment
    variables and substitute via your shell or a `.env` loader.

## Section: `local`

Local filesystem storage config. Used when the notebook sets `STORAGE_MODE = "local"`.

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
