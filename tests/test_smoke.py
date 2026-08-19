"""Smoke tests: do not require Docker / Postgres / MinIO."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from ducklake_playground import (
    PARTITION_COL,
    DuckLakeEngine,
    GeneratorSpec,
    PlaygroundConfig,
    StreamingGenerator,
    build_schema,
    load_config,
    measure_time_and_memory,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.yaml"


def test_load_config_yields_resolved_local_path() -> None:
    cfg = load_config(CONFIG_PATH)
    assert isinstance(cfg, PlaygroundConfig)
    assert Path(cfg.local.base_path).is_absolute(), (
        f"local.base_path should be resolved to absolute, got {cfg.local.base_path!r}"
    )
    assert cfg.local.base_path.endswith("data") or cfg.local.base_path.endswith("data/")


def test_schema_includes_id_and_event_date() -> None:
    cfg = load_config(CONFIG_PATH)
    schema = build_schema(cfg.schema)
    names = schema.names
    assert names[0] == cfg.schema.id_col
    assert names[1] == PARTITION_COL
    # Partition column must be NOT NULL.
    assert not schema.field(PARTITION_COL).nullable


def test_streaming_generator_yields_correct_row_count() -> None:
    cfg = load_config(CONFIG_PATH)
    gen = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=1000, chunk_size=300, seed=42))
    batches = list(gen.iter_batches())
    total = sum(b.num_rows for b in batches)
    assert total == 1000
    assert len(batches) == 4  # ceil(1000 / 300)


def test_streaming_generator_is_partition_sorted() -> None:
    """event_date must be monotonically non-decreasing across the entire stream."""
    cfg = load_config(CONFIG_PATH)
    gen = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=5000, chunk_size=1000, seed=42))
    flat = np.concatenate([b.column(PARTITION_COL).to_numpy(zero_copy_only=False) for b in gen.iter_batches()])
    assert bool(np.all(flat[:-1] <= flat[1:])), "event_date is not monotonic"


def test_generator_is_deterministic_for_fixed_seed() -> None:
    cfg = load_config(CONFIG_PATH)
    spec = GeneratorSpec(schema_config=cfg.schema, total_rows=500, chunk_size=200, seed=7)
    t1 = pa.Table.from_batches(list(StreamingGenerator(spec).iter_batches()))
    t2 = pa.Table.from_batches(list(StreamingGenerator(spec).iter_batches()))
    assert t1.equals(t2)


def test_generator_can_produce_a_fresh_deterministic_stream() -> None:
    """Each adapter call creates a new stream; Arrow readers themselves remain single-pass."""
    cfg = load_config(CONFIG_PATH)
    spec = GeneratorSpec(schema_config=cfg.schema, total_rows=500, chunk_size=200, seed=7)
    gen = StreamingGenerator(spec)
    first = pa.Table.from_batches(list(gen.iter_batches()))
    second = pa.Table.from_batches(list(gen.iter_batches()))
    assert first.equals(second)


def test_arrow_reader_round_trip() -> None:
    cfg = load_config(CONFIG_PATH)
    gen = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=300, chunk_size=100, seed=1))
    reader = gen.arrow_reader()
    table = pa.Table.from_batches(list(reader), schema=reader.schema)
    assert table.num_rows == 300
    assert PARTITION_COL in table.column_names


def test_merge_stream_overlap_ratio() -> None:
    cfg = load_config(CONFIG_PATH)
    n = 1000
    gen = StreamingGenerator(GeneratorSpec(schema_config=cfg.schema, total_rows=n, chunk_size=300, seed=42))
    reader = gen.merge_arrow_reader(0.10)
    table = pa.Table.from_batches(list(reader), schema=reader.schema)
    ids = table.column("id").to_numpy()
    assert table.num_rows == n
    updates = (ids < n).sum()
    inserts = (ids >= n).sum()
    assert updates == int(n * 0.10)
    assert inserts == n - updates


def test_unknown_column_type_raises() -> None:
    from ducklake_playground.config import ColumnDef, SchemaConfig

    with pytest.raises(ValueError, match="Unsupported column type"):
        SchemaConfig(
            id_col="id",
            seed=1,
            merge_overlap_ratio=0.0,
            columns=[ColumnDef(name="oops", type="not_a_real_type")],
        )


def test_schema_rejects_reserved_or_duplicate_column_names() -> None:
    from ducklake_playground.config import ColumnDef, SchemaConfig

    with pytest.raises(ValueError, match="must be unique"):
        SchemaConfig(
            id_col="id",
            seed=1,
            merge_overlap_ratio=0.0,
            columns=[ColumnDef(name="event_date", type="int64")],
        )


def test_column_def_rejects_invalid_type_specific_options() -> None:
    from ducklake_playground.config import ColumnDef

    with pytest.raises(ValueError, match="cardinality"):
        ColumnDef(name="category", type="varchar")


def test_measure_time_and_memory_returns_one_result() -> None:
    with measure_time_and_memory() as t:
        # Trivial allocation.
        _ = list(range(10_000))
    assert len(t) == 1
    assert t[0].wall_time_seconds >= 0
    assert t[0].peak_rss_mb > 0


def test_duckdb_version_comparison_is_numeric() -> None:
    assert DuckLakeEngine._version_tuple("1.10.0") > DuckLakeEngine._version_tuple("1.5.4")
