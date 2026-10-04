---
title: Kafka JSON Events
---

# Kafka JSON Events to DuckLake

The existing [streaming notebook](notebooks.md) pulls Arrow batches through one
large DuckLake write. This exercise sends individual JSON events through Kafka
and commits many small batches to DuckLake:

```text
StreamingGenerator → JSON producer → Kafka topic → Python consumer → DuckLake kafka_events
```

It uses the same schema from `config.yaml` and the same local Postgres-backed
DuckLake catalog as the notebook. The consumer writes a **new** table named
`kafka_events`; it does not change `demo_table`.

## Run it

Install dependencies with `just install` if this is a fresh checkout. Docker
must be running. In one terminal, start Postgres and Kafka:

```bash
just up-kafka
docker compose --profile kafka ps  # wait until Kafka and Postgres are healthy
```

The broker uses `localhost:9092`. In a second terminal, start the consumer:

```bash
just consume-events
```

In a third terminal, publish 12 new events. Every fourth event is sent twice,
so this command puts 15 messages on the topic:

```bash
just produce-events --count 12 --duplicate-every 4
```

Stop the consumer with Ctrl-C after it receives the messages. It flushes any
partial batch before exiting. You can also run a finite consumer after the
producer finishes:

```bash
just consume-events --idle-exit 10
```

Inspect the table and recent DuckLake snapshots:

```bash
just inspect-events
```

For interactive queries, open the same read-only walkthrough in either format:

```bash
just kafka-marimo    # notebooks/kafka_events.py
just kafka-jupyter   # notebooks/kafka_events.ipynb
```

Keep Postgres running while the notebook is open. The notebook reads the local
`kafka_events` table; the producer and consumer continue to run in their own
terminals. The table appears after the first consumer batch is written. Rerun
the notebook's query cells to see later batches, since external writes do not
trigger notebook reactivity.

For a fresh table, the result should show `rows=12, distinct_ids=12`. If you
have run the demo before, the table retains earlier events and both numbers
will be larger, but equal. Each successful microbatch creates a DuckLake
snapshot.

## What is in each message?

The Kafka **key** is the decimal `id`; the **value** is one UTF-8 JSON object
with exactly the fields in the configured Arrow schema. The producer reuses
`StreamingGenerator` for sample values, then assigns a fresh `id`, sets
`timestamp_col` to a UTC event time, and sets `event_date` from that time.
The JSON converter represents decimals and dates as strings and maps as
arrays of `[key, value]` pairs. The consumer converts those values back to
the configured Arrow types before writing.

Kafka's topic, partition, and offset are delivery metadata. They are not
columns in `kafka_events`.

## Replay and offsets

Start the consumer again with its default group (`ducklake-loader`). It
resumes from committed offsets, so it normally receives no old messages.
Then use a **new** group to deliberately replay the topic from its beginning:

```bash
just consume-events --group-id my-first-replay --idle-exit 10
just inspect-events
```

The total row count should stay the same. Each batch deduplicates repeated
IDs, then DuckLake `MERGE` updates an existing `id` or inserts a new one.
Use another new group name for another full replay.

The consumer turns off automatic Kafka offset commits. It commits offsets
only after DuckLake finishes a batch. A crash between those two steps can
cause Kafka to deliver the batch again; `MERGE` makes an exact replay safe.
Kafka and DuckLake do not share one transaction, so this is an **at-least-once**
pipeline rather than an end-to-end exactly-once pipeline. A changed event
with the same `id` replaces the old row.

## Batches and small writes

The consumer writes after 50 messages or 5 seconds, whichever comes first.
Try `just consume-events --batch-size 5 --batch-timeout 1` to see more
DuckLake snapshots. Small writes can be stored inline in the DuckLake
catalog instead of immediately producing Parquet files. The
[maintenance guide](maintenance.md) shows how to inspect and flush inlined
data.

Stop the local services when finished:

```bash
just down-kafka
```

Their data is retained for the next run. The first version uses one local
broker and one consumer. Flink, schema registry, and cloud services are
outside this exercise.
