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

    def __post_init__(self) -> None:
        """Reject unsupported or internally inconsistent generator definitions."""
        supported = {
            "int8", "int16", "int32", "int64", "float32", "float64", "decimal", "date", "datetime",
            "timestamp", "varchar", "text", "boolean", "list", "map", "struct",
        }
        if not self.name or self.type not in supported:
            raise ValueError(f"Unsupported column type: {self.type}")
        if self.type == "decimal" and not (1 <= self.precision <= 38 and 0 <= self.scale <= self.precision):
            raise ValueError("decimal precision must be 1..38 and scale must be 0..precision")
        if self.type == "varchar" and self.cardinality <= 0:
            raise ValueError("varchar cardinality must be positive")
        if self.type in {"text", "list", "map"} and self.avg_length <= 0:
            raise ValueError(f"{self.type} avg_length must be positive")
        if self.type == "list" and self.child_type != "int32":
            raise ValueError("list child_type must be int32")
        if self.type == "map" and (self.key_type != "varchar" or self.value_type != "int32"):
            raise ValueError("map key_type/value_type must be varchar/int32")
        if self.type == "struct":
            fields = [field.partition(":") for field in self.fields]
            if not fields or any(not name or separator != ":" or typ not in {"int32", "varchar", "float64"} for name, separator, typ in fields):
                raise ValueError("struct fields must be non-empty name:int32|varchar|float64 entries")
            if len({name for name, _, _ in fields}) != len(fields):
                raise ValueError("struct field names must be unique")


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

    def __post_init__(self) -> None:
        """Validate names that must remain unambiguous in generated SQL and Arrow schemas."""
        names = [self.id_col, "event_date", *(column.name for column in self.columns)]
        if not self.id_col:
            raise ValueError("id_col must not be empty")
        if len(set(names)) != len(names):
            raise ValueError("id_col, event_date, and user column names must be unique")
        if not 0.0 <= self.merge_overlap_ratio <= 1.0:
            raise ValueError("merge_overlap_ratio must be in [0, 1]")


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
    """S3-compatible storage config (e.g. MinIO). Used when the notebook selects ``storage_mode == 's3'``."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    ducklake_prefix: str


@dataclass(frozen=True)
class LocalConfig:
    """Local filesystem storage config. Used when the notebook selects ``storage_mode == 'local'``."""

    base_path: str
    ducklake_prefix: str


@dataclass(frozen=True)
class PlaygroundConfig:
    """Top-level playground configuration loaded from ``config.yaml``."""

    name: str
    batch_size: int
    target_file_size_mb: int
    parquet_row_group_size: int
    schema: SchemaConfig
    postgres: PostgresConfig
    s3: S3Config
    local: LocalConfig

    def __post_init__(self) -> None:
        """Validate operational settings before a notebook starts writing data."""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.target_file_size_mb <= 0:
            raise ValueError("target_file_size_mb must be positive")
        if self.parquet_row_group_size <= 0:
            raise ValueError("parquet_row_group_size must be positive")

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
    base_path_resolved = raw_base if Path(raw_base).is_absolute() else str((path.parent / raw_base).resolve())
    local = LocalConfig(
        base_path=base_path_resolved,
        ducklake_prefix=local_raw["ducklake_prefix"],
    )

    bench = raw["playground"]
    return PlaygroundConfig(
        name=bench["name"],
        batch_size=bench["batch_size"],
        target_file_size_mb=bench.get("target_file_size_mb", 16),
        parquet_row_group_size=bench.get("parquet_row_group_size", 122_880),
        schema=schema,
        postgres=postgres,
        s3=s3,
        local=local,
    )
