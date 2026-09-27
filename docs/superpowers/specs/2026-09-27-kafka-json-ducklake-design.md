# Kafka JSON to DuckLake learning pipeline

## Intent and scope

Add a small local event pipeline to learn Kafka production, consumption, batching,
offsets, replay, and duplicate handling. Preserve the existing Arrow streaming demo.
Use the same configured Arrow schema and `DuckLakeEngine`, but write to a new table
(`kafka_events`) in the existing local DuckLake catalog. The first version uses one
Kafka broker and no Flink, schema registry, MinIO, or cloud service.

Success means a learner can produce a finite run of JSON events, watch them arrive
in DuckLake in small commits, stop and restart the consumer, intentionally redeliver
some events, and verify that the table contains one current row per `id`.

## Data flow

1. A producer obtains rows from `StreamingGenerator` with a small chunk size. It
   retains the existing column set and types, assigns each new event a random
   positive 63-bit integer `id` (retrying any collision within the run), sets
   `timestamp_col` to a UTC event time, and derives
   `event_date` from that time. An intentionally repeated event reuses the same
   `id` and JSON value. The producer sends one row per Kafka message, keyed by
   the decimal representation of `id`.
2. A schema-aware codec converts Arrow rows to UTF-8 JSON and back. It uses the
   schema from `build_schema(config.schema)` as its contract: exact field names,
   typed conversion, and an error for any missing or extra field or an invalid
   required value. Nullable fields must be present, using JSON `null` when
   empty. Decimal
   values are strings, dates and timestamps use ISO 8601 strings, dictionary
   strings become JSON strings, and map values use arrays of key/value pairs so
   Arrow map semantics round-trip. Structs and lists use JSON objects and arrays.
   Kafka topic/partition/offset remain transport metadata; no new DuckLake
   columns are required.
3. A single consumer reads a small batch or flushes after a short timeout. It
   disables automatic offset commits, verifies that each Kafka key matches the
   JSON `id`, validates and converts every message to
   the configured Arrow schema, and deduplicates repeated `id` values within
   the batch. It creates `kafka_events` with that schema if absent, then applies
   the batch by `id` using DuckLake `MERGE` (the existing engine's upsert path).
   It synchronously commits consumed Kafka offsets only after the DuckLake write
   succeeds. On a decode or write failure, it reports the error and stops
   without committing that batch. An optional idle timeout lets a finite
   demonstration run exit after the topic is drained.
4. A verification command or notebook section compares total rows with distinct
   IDs, shows recent snapshots, and demonstrates that replaying messages leaves
   the logical row count unchanged. A new consumer group or offset reset makes
   replay explicit; restarting the same group normally resumes from its committed
   offsets.

## Delivery semantics and limitations

The DuckLake write and Kafka offset commit are separate operations. A crash after
the write but before the offset commit replays a batch. The `MERGE` keyed by `id`
makes exact duplicate events idempotent for this exercise. It is still an
at-least-once pipeline, not an end-to-end exactly-once transaction. A message
with a reused `id` and different values replaces that row; the producer only
creates exact duplicates in this exercise.

The existing `demo_table` and its generated data remain untouched. The event
producer changes `timestamp_col` and `event_date` only in its outgoing rows, so
the current generator and notebook keep their present behavior. DuckLake's
partitioning by `event_date` remains available. Small writes may be inlined into
the metadata catalog and can be flushed separately for inspection.

## Repository changes

- Add one Kafka service to the existing Docker Compose setup, keeping Postgres.
- Add the schema-aware JSON codec and narrow producer/consumer entry points.
- Reuse `load_config`, `StreamingGenerator`, `build_schema`, and `DuckLakeEngine`.
  Add only the small table-initialization helper the consumer needs if the
  existing engine API cannot create an empty table cleanly.
- Add a short walkthrough and `just` commands to start the broker, run the
  producer and consumer, replay events, and inspect the result.

## Verification

- Unit checks round-trip representative rows through JSON, including decimal,
  UTC timestamp, date, dictionary string, list, map, struct, and null values.
- Unit checks reject malformed records and deduplicate a batch by `id`.
- A local integration check runs Kafka and Postgres, produces a finite event
  set with deliberate duplicates, drains the consumer, and confirms
  `count(*) = count(distinct id)` before and after replay.

Flink is a later exercise. If added, its output contract must be designed as a
changelog or materialized upsert stream, not silently treated as append-only.
