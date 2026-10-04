"""Consume JSON Kafka events into a DuckLake table in replay-safe microbatches."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import TYPE_CHECKING, cast

from confluent_kafka import Consumer, KafkaError

from .event_json import decode_event, events_reader

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import pyarrow as pa

    from .engine import DuckLakeEngine


def write_batch(
    engine: DuckLakeEngine,
    table_name: str,
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema,
) -> int:
    """Merge the latest occurrence of each event ID in a microbatch."""
    latest_by_id = {cast("int", row["id"]): row for row in rows}
    if not latest_by_id:
        return 0
    engine.ensure_table(table_name, schema)
    engine.merge_upsert(table_name, events_reader(list(latest_by_id.values()), schema), "id")
    return len(latest_by_id)


def consume(
    consumer: Consumer,
    engine: DuckLakeEngine,
    schema: pa.Schema,
    *,
    table_name: str,
    batch_size: int,
    batch_timeout: float,
    idle_exit: float | None,
) -> int:
    """Read messages, write a microbatch, then synchronously commit offsets."""
    if batch_size <= 0 or batch_timeout <= 0 or (idle_exit is not None and idle_exit < 0):
        raise ValueError("batch_size and batch_timeout must be positive; idle_exit must be non-negative")
    rows: list[dict[str, object]] = []
    total = 0
    last_flush = time.monotonic()
    last_message = last_flush

    def flush() -> None:
        nonlocal last_flush
        if not rows:
            return
        write_batch(engine, table_name, rows, schema)
        consumer.commit(asynchronous=False)
        rows.clear()
        last_flush = time.monotonic()

    try:
        while True:
            message = consumer.poll(1.0)
            now = time.monotonic()
            if message is not None and (error := message.error()) is not None:
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    message = None  # The producer may create this topic after we subscribe.
                else:
                    raise RuntimeError(f"Kafka consumer error: {error}")
            if message is not None:
                payload = message.value()
                if payload is None:
                    raise ValueError("Kafka event has a null JSON value")
                row = decode_event(payload, schema)
                key = message.key()
                if key != str(row["id"]).encode("ascii"):
                    raise ValueError("Kafka key does not match JSON id")
                rows.append(row)
                total += 1
                last_message = now
            if rows and (len(rows) >= batch_size or now - last_flush >= batch_timeout):
                flush()
            if message is None and idle_exit is not None and now - last_message >= idle_exit:
                flush()
                break
    except KeyboardInterrupt:
        flush()
    finally:
        consumer.close()
        engine.close()
    return total


def main() -> None:
    """Run a single Kafka consumer against the local DuckLake catalog."""
    from .config import load_config
    from .data_generator import build_schema
    from .engine import DuckLakeEngine

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--batch-timeout", type=float, default=5.0)
    parser.add_argument("--idle-exit", type=float, default=None)
    parser.add_argument("--group-id", default="ducklake-loader")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--table", default="kafka_events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--inspect", action="store_true", help="query DuckLake without consuming Kafka")
    args = parser.parse_args()

    config = load_config(Path(__file__).resolve().parents[2] / "config.yaml")
    engine = DuckLakeEngine()
    engine.setup(config, "local")
    if args.inspect:
        try:
            fq = engine.qualified_table(args.table)
            row = engine.connection.execute(f"SELECT count(*), count(DISTINCT id) FROM {fq}").fetchone()
            if row is None:
                raise RuntimeError(f"Could not inspect {args.table}")
            print(f"{args.table}: rows={row[0]}, distinct_ids={row[1]}")
            snapshots = engine.connection.execute(
                f"SELECT snapshot_id, CAST(snapshot_time AS VARCHAR) FROM {engine.catalog_name}.snapshots() "
                "ORDER BY snapshot_id DESC LIMIT 10"
            ).fetchall()
            print("Recent snapshots (id, time):")
            for snapshot in snapshots:
                print(snapshot)
        finally:
            engine.close()
        return
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    total = consume(
        consumer,
        engine,
        build_schema(config.schema),
        table_name=args.table,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout,
        idle_exit=args.idle_exit,
    )
    print(f"Consumed {total} Kafka messages into {args.table}")


if __name__ == "__main__":
    main()
