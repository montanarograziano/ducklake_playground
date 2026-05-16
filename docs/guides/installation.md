---
title: Installation
marimo-version: 0.23.4
---

# Installation

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12+ | Runtime |
| [uv](https://docs.astral.sh/uv/) | latest | Package manager |
| Docker | latest | DuckLake catalog backend (Postgres) |
| [just](https://github.com/casey/just) | optional | Task runner used in this README |

DuckDB 1.5.2+ (with the `ducklake` extension) is installed automatically by `uv sync`.
The extension itself is installed by the engine on first connection.

## Clone + install

```bash
git clone https://github.com/montanarograziano/ducklake_playground.git
cd ducklake_playground

# Install all groups (runtime + dev + docs)
just install
# equivalent to: uv sync --all-groups
```

This creates a `.venv` in the repo root with all dependencies pinned.

## Start the DuckLake catalog (Postgres)

DuckLake stores its catalog metadata in PostgreSQL. The `docker-compose.yml` in the repo
root provisions a Postgres 17 container with sensible defaults.

### Optional: override defaults via `.env`

The compose file reads five environment variables — all of which have defaults baked
into `docker-compose.yml`, so this step is optional:

| Variable | Default | Used by |
|----------|---------|---------|
| `POSTGRES_DB` | `ducklake_playground` | Postgres |
| `POSTGRES_USER` | `user` | Postgres |
| `POSTGRES_PASSWORD` | `password` | Postgres |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO (s3 profile) |
| `AWS_SECRET_ACCESS_KEY` | `miniopassword` | MinIO (s3 profile) |

To override:

```bash
cp .env.example .env
# edit .env
```

!!! warning "Mirror changes in config.yaml"
    The Python engine reads connection details from `config.yaml`, not from the environment.
    If you change any credential in `.env`, update the matching field in `config.yaml`
    (`postgres.*` and `s3.*` sections) so the engine connects to the same Postgres / MinIO
    instance compose is running.

### Start

```bash
just up
# equivalent to: docker compose up -d
```

Verify Postgres is ready:

```bash
PGPASSWORD=password psql -h localhost -p 5432 -U user -d postgres -c "SELECT 1"
```

You should see a single-row result.

The engine's `setup()` method auto-creates the catalog database on first run, so no
manual `CREATE DATABASE` is needed.

## Optional: MinIO (for S3-backed runs)

If you want to demo DuckLake against S3-compatible storage instead of the local
filesystem, start MinIO too:

```bash
just up-s3
# equivalent to: docker compose --profile s3 up -d
```

MinIO console: <http://localhost:9001> (login `minioadmin` / `miniopassword`).

Then in the notebook, set:

```python
STORAGE_MODE = "s3"
```

…or call `engine.setup(config, "s3")` directly. The credentials and bucket come
from the `s3:` section of `config.yaml`.

## Verify the install

```bash
uv run python -c "
from ducklake_playground import load_config, StreamingGenerator, GeneratorSpec
cfg = load_config('config.yaml')
gen = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=1000, chunk_size=100, seed=42))
print('schema fields:', len(gen.schema))
print('first batch:', next(gen.iter_batches()).num_rows, 'rows')
"
```

Expected output:

```
schema fields: 17
first batch: 100 rows
```

If the import fails, you most likely skipped `just install` or your shell is using a
different Python than the one in `.venv`. Re-run `just install` and ensure the prompt
is using the project's environment (`source .venv/bin/activate` or `uv run ...`).

## Troubleshooting

### `ModuleNotFoundError: No module named 'ducklake_playground'`

You probably ran `python` directly. Use `uv run python ...` or activate the venv:

```bash
source .venv/bin/activate
```

### `psql` not found in PATH

The engine uses `psql` to create the catalog database on first connect. On macOS:

```bash
brew install libpq
brew link --force libpq
```

On Debian/Ubuntu:

```bash
sudo apt install postgresql-client
```

### Postgres port 5432 already in use

Edit `docker-compose.yml` and change the host port mapping (the container side stays 5432):

```yaml
ports:
  - "5433:5432"   # host:container
```

Then update `config.yaml`:

```yaml
postgres:
  port: 5433
```

### DuckDB extension fetch fails

The first run downloads the `ducklake` and `postgres` DuckDB extensions. If you're
behind a proxy or air-gapped, you'll see a network error. Set `DUCKDB_EXTENSION_REPO`
or pre-install the extension binaries.
