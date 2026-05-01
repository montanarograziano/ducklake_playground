"""YAML-driven configuration for the DuckLake playground.

Configuration is split into immutable dataclasses so the typed surface is the same shape as
the YAML. ``load_config`` is the single entry point: pass a ``Path`` or ``str`` and get back
a fully populated ``PlaygroundConfig``.

Path resolution policy: ``local.base_path`` is resolved against the **YAML file's directory**
(not the current working directory). This makes the data location identical regardless of
where the notebook is launched from (Jupyter, marimo, CLI).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ColumnDef:
    """Single column in the synthetic schema.

    Attributes:
        name: Column name as it appears in the table.
        type: One of ``int8``, ``int16``, ``int32``, ``int64``, ``float32``, ``float64``,
            ``decimal``, ``date``, ``datetime``, ``timestamp``, ``varchar``, ``text``,
            ``boolean``, ``list``, ``struct``, ``map``.
        precision: For ``decimal``: total digits.
        scale: For ``decimal``: digits after the decimal point.
        cardinality: For ``varchar``: distinct value pool size (dictionary-encoded).
        avg_length: For ``text``/``list``/``map``: target average length.
        child_type: For ``list``: element type (currently always ``int32``).
        fields: For ``struct``: list of ``"name:type"`` strings.
        key_type: For ``map``: key type (currently always ``varchar``).
        value_type: For ``map``: value type (currently always ``int32``).
    """

    name: str
    type: str
    precision: int = 0
    scale: int = 0
    cardinality: int = 0
    avg_length: int = 0
    child_type: str = ""
    fields: list[str] = field(default_factory=list)
    key_type: str = ""
    value_type: str = ""


@dataclass(frozen=True)
class SchemaConfig:
    """Synthetic schema configuration.

    The streaming generator always injects two non-listed columns:

    * ``id_col`` (``int64``, NOT NULL): sequential row id.
    * ``event_date`` (``pa.date32``, NOT NULL): partition column.

    The ``columns`` list adds user-defined columns for type coverage.

    Attributes:
        id_col: Name of the primary-key column injected by the generator.
        seed: Base RNG seed; per-chunk seeds are derived via ``np.random.SeedSequence.spawn``.
        merge_overlap_ratio: Fraction of merge-source rows that reuse existing IDs (rest are
            new inserts). Used by ``StreamingGenerator.iter_merge_batches``.
        columns: User-defined columns appended to the canonical (id, event_date) pair.
    """

    id_col: str
    seed: int
    merge_overlap_ratio: float
    columns: list[ColumnDef]


@dataclass(frozen=True)
class FilterConfig:
    """Predicate used by the engine's filtered-scan helper.

    The date range applies to the partition column ``event_date``, exercising partition
    pruning. The varchar values are matched with ``IN (...)``.
    """

    date_range: tuple[str, str]
    varchar_values: list[str]


@dataclass(frozen=True)
class PostgresConfig:
    """Connection details for the DuckLake metadata catalog (PostgreSQL)."""

    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class S3Config:
    """S3-compatible storage config (e.g. MinIO). Used only when ``storage_mode == 's3'``."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    ducklake_prefix: str


@dataclass(frozen=True)
class LocalConfig:
    """Local filesystem storage config. Used when ``storage_mode == 'local'``."""

    base_path: str
    ducklake_prefix: str


@dataclass(frozen=True)
class PlaygroundConfig:
    """Top-level playground configuration loaded from ``config.yaml``."""

    name: str
    batch_size: int
    target_file_size_mb: int
    parquet_row_group_size: int
    storage_modes: list[str]
    default_storage_mode: str
    schema: SchemaConfig
    filter: FilterConfig
    postgres: PostgresConfig
    s3: S3Config
    local: LocalConfig

    @property
    def target_file_size_bytes(self) -> int:
        """Target Parquet file size in bytes (engine writer parameter)."""
        return self.target_file_size_mb * 1024 * 1024


def load_config(config_path: str | Path) -> PlaygroundConfig:
    """Load and validate the playground configuration from a YAML file.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        A fully-populated, immutable ``PlaygroundConfig`` instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        KeyError: If a required key is missing from the YAML.
    """
    path = Path(config_path)
    with path.open() as f:
        raw = yaml.safe_load(f)

    columns = [ColumnDef(**col) for col in raw["schema"]["columns"]]
    schema = SchemaConfig(
        id_col=raw["schema"]["id_col"],
        seed=raw["schema"]["seed"],
        merge_overlap_ratio=raw["schema"]["merge_overlap_ratio"],
        columns=columns,
    )

    filter_cfg = FilterConfig(
        date_range=tuple(raw["filter"]["date_range"]),
        varchar_values=raw["filter"]["varchar_values"],
    )

    pg = raw["postgres"]
    postgres = PostgresConfig(
        host=pg["host"],
        port=pg["port"],
        database=pg["database"],
        user=pg["user"],
        password=pg["password"],
    )

    s3_raw = raw["s3"]
    s3 = S3Config(
        endpoint=s3_raw["endpoint"],
        access_key=s3_raw["access_key"],
        secret_key=s3_raw["secret_key"],
        bucket=s3_raw["bucket"],
        ducklake_prefix=s3_raw["ducklake_prefix"],
    )

    local_raw = raw["local"]
    raw_base = local_raw["base_path"]
    # Resolve relative paths against the YAML file's directory, not cwd. This keeps the
    # data location stable across notebook entry points (Jupyter sets cwd to the notebook
    # dir; CLI runs from the repo root; both should land at the same absolute path).
    base_path_resolved = (
        raw_base if Path(raw_base).is_absolute() else str((path.parent / raw_base).resolve())
    )
    local = LocalConfig(
        base_path=base_path_resolved,
        ducklake_prefix=local_raw["ducklake_prefix"],
    )

    bench = raw["playground"]
    storage_modes = raw["storage_modes"]
    default_storage_mode = raw.get("default_storage_mode") or storage_modes[0]
    return PlaygroundConfig(
        name=bench["name"],
        batch_size=bench["batch_size"],
        target_file_size_mb=bench.get("target_file_size_mb", 16),
        parquet_row_group_size=bench.get("parquet_row_group_size", 122_880),
        storage_modes=storage_modes,
        default_storage_mode=default_storage_mode,
        schema=schema,
        filter=filter_cfg,
        postgres=postgres,
        s3=s3,
        local=local,
    )
