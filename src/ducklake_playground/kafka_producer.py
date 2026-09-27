"""Publish finite runs of playground rows as JSON Kafka events."""

from __future__ import annotations

import argparse
import datetime as dt
import secrets
from pathlib import Path
from typing import TYPE_CHECKING

from .data_generator import GeneratorSpec, StreamingGenerator
from .event_json import encode_event

if TYPE_CHECKING:
    from collections.abc import Iterator

    from .config import PlaygroundConfig


def iter_events(config: PlaygroundConfig, count: int, duplicate_every: int) -> Iterator[dict[str, object]]:
    """Yield new event rows plus exact duplicates after each N new events."""
    if count < 0 or duplicate_every < 0:
        raise ValueError("count and duplicate_every must be non-negative")
    generator = StreamingGenerator(
        GeneratorSpec(
            schema_config=config.schema, total_rows=count, chunk_size=min(100, max(1, count)), seed=config.schema.seed
        )
    )
    used_ids: set[int] = set()
    emitted = 0
    for batch in generator.iter_batches():
        for row in batch.to_pylist():
            event_id = secrets.randbelow(2**63 - 1) + 1
            while event_id in used_ids:
                event_id = secrets.randbelow(2**63 - 1) + 1
            used_ids.add(event_id)
            now = dt.datetime.now(dt.UTC)
            row[config.schema.id_col] = event_id
            row["timestamp_col"] = now
            row["event_date"] = now.date()
            yield row
            emitted += 1
            if duplicate_every and emitted % duplicate_every == 0:
                yield row.copy()


def main() -> None:
    """Publish one JSON row per Kafka message to a local broker."""
    from confluent_kafka import Producer

    from .config import load_config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=150)
    parser.add_argument("--duplicate-every", type=int, default=5)
    parser.add_argument("--topic", default="events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    config = load_config(Path(__file__).resolve().parents[2] / "config.yaml")
    schema = StreamingGenerator(GeneratorSpec(config.schema, total_rows=0, chunk_size=1)).schema
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    failures: list[str] = []

    def delivery_report(error: object, _message: object) -> None:
        if error is not None:
            failures.append(str(error))

    sent = 0
    for row in iter_events(config, args.count, args.duplicate_every):
        producer.produce(
            args.topic,
            key=str(row[config.schema.id_col]).encode("ascii"),
            value=encode_event(row, schema),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1
    undelivered = producer.flush(30)
    if failures or undelivered:
        raise RuntimeError(f"Kafka delivery failed: {len(failures)} errors, {undelivered} undelivered messages")
    print(f"Published {sent} messages to {args.topic} ({args.count} unique events)")


if __name__ == "__main__":
    main()
