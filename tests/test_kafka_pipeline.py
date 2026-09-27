"""Behavior at the producer and consumer boundaries."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from ducklake_playground import load_config
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
