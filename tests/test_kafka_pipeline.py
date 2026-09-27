"""Behavior at the producer and consumer boundaries."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from ducklake_playground import load_config
from ducklake_playground.event_json import encode_event
from ducklake_playground.kafka_consumer import consume
from ducklake_playground.kafka_producer import iter_events


def test_producer_repeats_exact_events_without_new_ids() -> None:
    config = load_config(Path(__file__).parents[1] / "config.yaml")
    events = list(iter_events(config, count=12, duplicate_every=4))
    assert len(events) == 15
    assert events[4] == events[3]
    assert events[9] == events[8]
    assert events[14] == events[13]
    originals = [event for index, event in enumerate(events) if index not in {4, 9, 14}]
    assert len({event["id"] for event in originals}) == 12
    assert all(isinstance(event["id"], int) and event["id"] > 0 for event in originals)
    assert all(event["event_date"] == event["timestamp_col"].date() for event in originals)
    assert all(event["timestamp_col"].tzinfo == dt.UTC for event in originals)
    expected_fields = {config.schema.id_col, "event_date", *(column.name for column in config.schema.columns)}
    assert all(set(event) == expected_fields for event in originals)


def test_producer_can_emit_without_duplicates() -> None:
    config = load_config(Path(__file__).parents[1] / "config.yaml")
    assert len(list(iter_events(config, count=3, duplicate_every=0))) == 3


class FakeMessage:
    def __init__(self, key: bytes, value: bytes) -> None:
        self._key = key
        self._value = value

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = messages.copy()
        self.commits: list[bool] = []
        self.closed = False

    def poll(self, _timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, asynchronous: bool) -> None:
        self.commits.append(asynchronous)

    def close(self) -> None:
        self.closed = True


class FakeEngine:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.merged: list[dict] = []
        self.created: list[str] = []
        self.closed = False

    def ensure_table(self, table_name: str, _schema: object) -> None:
        self.created.append(table_name)

    def merge_upsert(self, _table_name: str, reader: object, _key: str) -> None:
        if self.fail:
            raise RuntimeError("write failed")
        self.merged.extend(batch.to_pylist()[0] for batch in reader)

    def close(self) -> None:
        self.closed = True


def _event_message() -> tuple[FakeMessage, object]:
    config = load_config(Path(__file__).parents[1] / "config.yaml")
    from ducklake_playground import build_schema

    schema = build_schema(config.schema)
    row = next(iter_events(config, count=1, duplicate_every=0))
    return FakeMessage(str(row["id"]).encode(), encode_event(row, schema)), schema


def test_consumer_deduplicates_batch_before_commit() -> None:
    message, schema = _event_message()
    consumer = FakeConsumer([message, message])
    engine = FakeEngine()
    count = consume(consumer, engine, schema, table_name="kafka_events", batch_size=2, batch_timeout=5, idle_exit=0)
    assert count == 2
    assert len(engine.merged) == 1
    assert engine.created == ["kafka_events"]
    assert consumer.commits == [False]
    assert consumer.closed and engine.closed


def test_consumer_rejects_mismatched_key_without_commit() -> None:
    message, schema = _event_message()
    consumer = FakeConsumer([FakeMessage(b"999", message.value())])
    engine = FakeEngine()
    with pytest.raises(ValueError, match="key"):
        consume(consumer, engine, schema, table_name="kafka_events", batch_size=1, batch_timeout=5, idle_exit=0)
    assert engine.merged == []
    assert consumer.commits == []


def test_consumer_rejects_malformed_json_without_commit() -> None:
    _, schema = _event_message()
    consumer = FakeConsumer([FakeMessage(b"1", b"not json")])
    engine = FakeEngine()
    with pytest.raises(ValueError, match="JSON"):
        consume(consumer, engine, schema, table_name="kafka_events", batch_size=1, batch_timeout=5, idle_exit=0)
    assert engine.merged == []
    assert consumer.commits == []


def test_consumer_does_not_commit_failed_write() -> None:
    message, schema = _event_message()
    consumer = FakeConsumer([message])
    engine = FakeEngine(fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        consume(consumer, engine, schema, table_name="kafka_events", batch_size=1, batch_timeout=5, idle_exit=0)
    assert consumer.commits == []


def test_consumer_flushes_partial_batch_on_idle_exit() -> None:
    message, schema = _event_message()
    consumer = FakeConsumer([message])
    engine = FakeEngine()
    count = consume(consumer, engine, schema, table_name="kafka_events", batch_size=2, batch_timeout=5, idle_exit=0)
    assert count == 1
    assert len(engine.merged) == 1
    assert consumer.commits == [False]
