"""The Kafka JSON boundary preserves the configured Arrow schema."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from ducklake_playground import GeneratorSpec, StreamingGenerator, load_config
from ducklake_playground.event_json import decode_event, encode_event, events_reader


@pytest.fixture
def sample() -> tuple[dict, pa.Schema]:
    config = load_config(Path(__file__).parents[1] / "config.yaml")
    batch = next(StreamingGenerator(GeneratorSpec(config.schema, total_rows=1, chunk_size=1, seed=42)).iter_batches())
    return batch.to_pylist()[0], batch.schema


def test_round_trip_preserves_complex_arrow_row(sample: tuple[dict, pa.Schema]) -> None:
    row, schema = sample
    payload = encode_event(row, schema)
    json_row = json.loads(payload)
    assert isinstance(json_row["decimal_col"], str)
    assert json_row["event_date"] == "2024-01-01"
    assert json_row["timestamp_col"].endswith("+00:00")
    assert isinstance(json_row["map_col"][0], list)

    decoded = decode_event(payload, schema)
    actual = pa.Table.from_batches(list(events_reader([decoded], schema)), schema=schema)
    expected = pa.Table.from_pylist([row], schema=schema)
    assert actual.equals(expected)


def test_nullable_field_round_trips_as_json_null(sample: tuple[dict, pa.Schema]) -> None:
    row, schema = sample
    row["struct_col"] = None
    assert json.loads(encode_event(row, schema))["struct_col"] is None
    assert decode_event(encode_event(row, schema), schema)["struct_col"] is None


@pytest.mark.parametrize("change", ["missing", "extra", "null_id"])
def test_rejects_schema_mismatch(sample: tuple[dict, pa.Schema], change: str) -> None:
    row, schema = sample
    if change == "missing":
        del row["text_col"]
    elif change == "extra":
        row["surprise"] = 1
    else:
        row["id"] = None
    with pytest.raises(ValueError):
        encode_event(row, schema)


def test_rejects_invalid_json_types(sample: tuple[dict, pa.Schema]) -> None:
    row, schema = sample
    raw = json.loads(encode_event(row, schema))
    raw["decimal_col"] = "not-a-decimal"
    with pytest.raises(ValueError, match="decimal_col"):
        decode_event(json.dumps(raw).encode(), schema)
    raw["decimal_col"] = "1.0000"
    raw["map_col"] = [["key_only"]]
    with pytest.raises(ValueError, match="map_col"):
        decode_event(json.dumps(raw).encode(), schema)
