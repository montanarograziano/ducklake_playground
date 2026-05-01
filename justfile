set positional-arguments

# List all available recipes
@help:
    just --list

# Install dependencies (creates .venv via uv)
install:
    uv sync --all-groups

# Update dependencies
update:
    uv sync --all-groups --upgrade

# Lint + type check
lint:
    uv run ruff format src tests
    uv run ruff check src tests --fix --unsafe-fixes
    uv run mypy src

# Run unit tests (no docker required)
test:
    uv run pytest tests -m "not integration"

# Run integration tests (requires docker compose up)
test-integration:
    uv run pytest tests -m "integration"

# Start Postgres only (default — needed for DuckLake catalog)
up:
    docker compose up -d

# Start Postgres + MinIO (for storage_mode=s3)
up-s3:
    docker compose --profile s3 up -d

# Stop containers (volumes preserved)
down:
    docker compose --profile s3 down

# Stop containers and wipe Postgres / MinIO data (fresh start)
down-clean:
    docker compose --profile s3 down -v
    rm -rf configs/postgres/data configs/minio/data data/

# Tail compose logs
logs:
    docker compose logs --follow

# Open the marimo demo notebook
demo:
    uv run marimo edit notebooks/streaming_demo.py

# Open the Jupyter demo notebook
jupyter:
    uv run jupyter lab notebooks/streaming_demo.ipynb

# Live preview the docs
preview-docs:
    uv run zensical serve

# Build the docs site
build-docs:
    uv run zensical build
