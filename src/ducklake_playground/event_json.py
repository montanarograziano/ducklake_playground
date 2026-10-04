"""JSON representation of rows in the playground's configured Arrow schema."""

from __future__ import annotations

import datetime as dt
import json
import math
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


def _check_fields(row: Mapping[str, object], schema: pa.Schema) -> None:
    expected = set(schema.names)
    actual = set(row)
    if actual != expected:
        raise ValueError(
            f"Event fields differ from schema: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    for field in schema:
        if row[field.name] is None and not field.nullable:
            raise ValueError(f"{field.name} must not be null")


def _convert(value: Any, typ: pa.DataType, path: str, *, decoding: bool) -> Any:
    if value is None:
        return None
    try:
        if pa.types.is_dictionary(typ):
            return _convert(value, cast("pa.DataType", typ.value_type), path, decoding=decoding)
        if pa.types.is_decimal(typ):
            if decoding and not isinstance(value, str):
                raise TypeError("expected a decimal string")
            number = Decimal(value)
            if not number.is_finite():
                raise ValueError("non-finite decimal")
            return number if decoding else str(number)
        if pa.types.is_date(typ):
            if decoding:
                if not isinstance(value, str):
                    raise TypeError("expected an ISO date string")
                return dt.date.fromisoformat(value)
            if isinstance(value, dt.datetime) or not isinstance(value, dt.date):
                raise TypeError("expected a date")
            return value.isoformat()
        if pa.types.is_timestamp(typ):
            if decoding:
                if not isinstance(value, str):
                    raise TypeError("expected an ISO timestamp string")
                value = dt.datetime.fromisoformat(value)
            if not isinstance(value, dt.datetime):
                raise TypeError("expected a datetime")
            if (value.tzinfo is not None) != (typ.tz is not None):
                raise ValueError("timezone does not match schema")
            if typ.tz is not None:
                value = value.astimezone(dt.UTC)
            return value if decoding else value.isoformat()
        if pa.types.is_map(typ):
            if not isinstance(value, list):
                raise TypeError("expected map as key/value pairs")
            pairs = []
            for pair in value:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise TypeError("expected [key, value] pair")
                key = _convert(pair[0], cast("pa.DataType", typ.key_type), f"{path}.key", decoding=decoding)
                item = _convert(pair[1], cast("pa.DataType", typ.item_type), f"{path}.value", decoding=decoding)
                pairs.append((key, item) if decoding else [key, item])
            return pairs
        if pa.types.is_list(typ) or pa.types.is_large_list(typ):
            if not isinstance(value, list):
                raise TypeError("expected list")
            return [
                _convert(item, cast("pa.DataType", typ.value_type), f"{path}[]", decoding=decoding) for item in value
            ]
        if pa.types.is_struct(typ):
            if not isinstance(value, dict) or set(value) != set(typ.names):
                raise TypeError("struct fields differ from schema")
            return {
                field.name: _convert(value[field.name], field.type, f"{path}.{field.name}", decoding=decoding)
                for field in typ
            }
        if pa.types.is_boolean(typ):
            if not isinstance(value, bool):
                raise TypeError("expected boolean")
            return value
        if pa.types.is_integer(typ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError("expected integer")
            return value
        if pa.types.is_floating(typ):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise TypeError("expected finite number")
            return value
        if pa.types.is_string(typ) or pa.types.is_large_string(typ):
            if not isinstance(value, str):
                raise TypeError("expected string")
            return value
        raise TypeError(f"unsupported Arrow type {typ}")
    except (TypeError, ValueError, InvalidOperation, OverflowError) as exc:
        raise ValueError(f"Invalid {path}: {exc}") from exc


def encode_event(row: Mapping[str, object], schema: pa.Schema) -> bytes:
    """Encode one typed Arrow-compatible row as a UTF-8 JSON object."""
    _check_fields(row, schema)
    encoded = {field.name: _convert(row[field.name], field.type, field.name, decoding=False) for field in schema}
    try:
        return json.dumps(encoded, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid event JSON: {exc}") from exc


def decode_event(payload: bytes, schema: pa.Schema) -> dict[str, object]:
    """Validate JSON fields and restore values expected by PyArrow."""
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid event JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("Event JSON must be an object")
    _check_fields(raw, schema)
    decoded = {field.name: _convert(raw[field.name], field.type, field.name, decoding=True) for field in schema}
    events_reader([decoded], schema)
    return decoded


def events_reader(rows: Sequence[Mapping[str, object]], schema: pa.Schema) -> pa.RecordBatchReader:
    """Build a schema-checked Arrow reader for one DuckLake microbatch."""
    for row in rows:
        _check_fields(row, schema)
    try:
        table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise ValueError(f"Event batch does not match Arrow schema: {exc}") from exc
    return table.to_reader()
