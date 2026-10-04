# Kafka JSON to DuckLake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream one JSON row per Kafka message into a new DuckLake table that uses the playground's existing Arrow schema, and make replay safe by `id`.

**Architecture:** The current `StreamingGenerator` supplies sample values. A small producer adapter gives each event a fresh ID and event time, a shared codec converts between Arrow-compatible Python values and JSON, and a single consumer batches records before calling `DuckLakeEngine.merge_upsert`. Kafka offsets are committed only after a successful DuckLake write.

**Tech Stack:** Python 3.12+, DuckDB/DuckLake, PyArrow, `confluent-kafka`, Apache Kafka in Docker Compose, pytest, `uv`, `just`.

**Spec:** `docs/superpowers/specs/2026-09-27-kafka-json-ducklake-design.md`

## Global Constraints

- Use a new table named `kafka_events` in the existing local DuckLake catalog; leave `demo_table` and the notebook's data flow unchanged.
- Use one JSON event per Kafka message, with decimal `id` as the Kafka key. Do not add Kafka metadata columns to the DuckLake schema.
- The first version has one Kafka broker and no Flink, schema registry, MinIO, or cloud service.
- A valid JSON event has exactly the fields in `build_schema(config.schema)`. Nullable fields are present as `null`; `id` and `event_date` are non-null.
- The pipeline is at least once. A replayed event with the same `id` is an upsert; a changed payload with the same `id` replaces the row.

## File map

- `src/ducklake_playground/event_json.py`: schema-aware row encoder/decoder and Arrow batch builder. No Kafka or DuckLake dependency.
- `src/ducklake_playground/kafka_producer.py`: event-time/ID adapter and finite Kafka producer CLI.
- `src/ducklake_playground/kafka_consumer.py`: single-consumer batching, DuckLake write, offset commit, and inspection CLI.
- `src/ducklake_playground/engine.py`: one public `ensure_table` method using its existing private table creation logic.
- `docker-compose.yml`, `pyproject.toml`, `justfile`: local broker and commands.
- `docs/guides/kafka-streaming.md`, `README.md`, `zensical.toml`: learning walkthrough and links.
- `tests/test_event_json.py`, `tests/test_kafka_pipeline.py`, `tests/test_kafka_integration.py`: codec, processing semantics, and optional Docker check.

## Review Focus

- JSON with missing/extra fields or malformed nested values must fail before the consumer commits offsets (Task 1 and Task 3 tests).
- A Kafka key that differs from the JSON `id` must fail before writing (Task 3 test).
- Two occurrences of an `id` in one batch must become one merge source row (Task 3 test).
- An idle exit with a partial batch must write and commit that batch (Task 3 test).
- A write failure must leave the batch offsets uncommitted so restart can replay it (Task 3 test).

---

### Task 1: JSON contract for the existing schema

**Files:**
- Create: `src/ducklake_playground/event_json.py`
- Create: `tests/test_event_json.py`

**Interfaces:**
- Consumes: `pa.Schema` from `build_schema(load_config(...).schema)`.
- Produces: `encode_event(row: Mapping[str, object], schema: pa.Schema) -> bytes`, `decode_event(payload: bytes, schema: pa.Schema) -> dict[str, object]`, and `events_reader(rows: Sequence[Mapping[str, object]], schema: pa.Schema) -> pa.RecordBatchReader`.

- [ ] **Step 1: Write failing round-trip and rejection tests.** Construct a row from `StreamingGenerator(...).iter_batches()` with `batch.slice(0, 1).to_pylist()[0]`. Assert that `decode_event(encode_event(row, schema), schema)` becomes the same typed Arrow row after `events_reader`, including `decimal_col`, `timestamp_col`, `event_date`, `varchar_col`, `list_col`, `map_col`, and `struct_col`. Assert JSON uses a decimal string, ISO date/time strings, and map key/value pairs. Also test a nullable field set to `None`, a missing field, an extra field, an invalid decimal, and an invalid map pair.

  ```python
  original = batch.slice(0, 1).to_pylist()[0]
  decoded = decode_event(encode_event(original, schema), schema)
  actual = pa.Table.from_batches(list(events_reader([decoded], schema)), schema=schema)
  expected = pa.Table.from_pylist([original], schema=schema)
  assert actual.equals(expected)
  ```
- [ ] **Step 2: Run `uv run pytest tests/test_event_json.py -q`; expect import/function failures.**
- [ ] **Step 3: Implement the codec.** Compare `set(row)` to `set(schema.names)` before conversion and explicitly reject `None` for each non-nullable Arrow field. Recursively encode/decode by `pa.DataType`: primitives as JSON scalars, `Decimal` as string, dates/times as ISO strings, dictionary values as their scalar value, list/struct recursively, and map as an ordered array of `[key, value]` pairs. Decode via explicit Python `Decimal`, `date`, and `datetime` constructors, then call `pa.Table.from_pylist(..., schema=schema)` in `events_reader` to enforce Arrow types. Raise `ValueError` with the field name on invalid input. Use `json.dumps(..., allow_nan=False, separators=(",", ":"))` and UTF-8 bytes.

  ```python
  def events_reader(rows: Sequence[Mapping[str, object]], schema: pa.Schema) -> pa.RecordBatchReader:
      table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
      return table.to_reader()
  ```
- [ ] **Step 4: Run `uv run pytest tests/test_event_json.py -q`; expect pass.** Run `uv run ruff check src/ducklake_playground/event_json.py tests/test_event_json.py` and `uv run ruff format --check` for those files.
- [ ] **Step 5: Commit the codec and tests:** `git add src/ducklake_playground/event_json.py tests/test_event_json.py && git commit -m "feat: encode playground events as JSON"`.

### Task 2: Finite producer and local Kafka broker

**Files:**
- Create: `src/ducklake_playground/kafka_producer.py`
- Modify: `docker-compose.yml`, `pyproject.toml`, `uv.lock`, `justfile`
- Test: `tests/test_kafka_pipeline.py`

**Interfaces:**
- Consumes: Task 1 `encode_event`; existing `StreamingGenerator` and configured schema.
- Produces: `iter_events(config: PlaygroundConfig, count: int, duplicate_every: int) -> Iterator[dict[str, object]]` and CLI `python -m ducklake_playground.kafka_producer --count 150 --duplicate-every 5`.

- [ ] **Step 1: Write failing producer tests.** Assert `iter_events(config, 12, 4)` yields 12 new events plus 3 exact resends; each new event has a distinct positive `id`, `event_date == timestamp_col.date()`, all configured fields, and a UTC `timestamp_col`. Patch the clock/ID source or inject them as private helpers so the test is deterministic. Assert `duplicate_every=0` yields no resends.

  ```python
  events = list(iter_events(config, count=12, duplicate_every=4))
  assert len(events) == 15
  assert events[4] == events[3]
  assert events[9] == events[8]
  assert events[14] == events[13]
  ```
- [ ] **Step 2: Run `uv run pytest tests/test_kafka_pipeline.py -q`; expect import/function failures.**
- [ ] **Step 3: Add the producer.** Generate a small Arrow batch (for example 100 rows) at a time; replace only `id`, `timestamp_col`, and `event_date` in outgoing Python rows. Use `secrets.randbelow(2**63 - 1) + 1`, retrying IDs used in this run. For every Nth new event, immediately resend the same row without assigning another ID. Send `key=str(row["id"]).encode()` and `value=encode_event(row, schema)` through `confluent_kafka.Producer`, handle delivery errors, and `flush()` before exit. Use command options for count, duplicate interval, topic, and broker address; default to a local `events` topic.

  ```python
  producer.produce(topic, key=str(row["id"]).encode("ascii"), value=encode_event(row, schema))
  producer.poll(0)
  ```
- [ ] **Step 4: Add the Kafka service and dependency.** Add a single-node KRaft Kafka service to `docker-compose.yml` with a host listener at `localhost:9092` and no fixed container name. Add `confluent-kafka` to project dependencies and update the lockfile with `uv lock`. Add `just up-kafka` and `just produce-events` commands. Keep existing `just up` usable for Postgres-only notebook work.
- [ ] **Step 5: Run `uv run pytest tests/test_kafka_pipeline.py -q` and `docker compose config`; expect pass.** Run lint/type checks on changed Python files.
- [ ] **Step 6: Commit:** `git add src/ducklake_playground/kafka_producer.py tests/test_kafka_pipeline.py docker-compose.yml pyproject.toml uv.lock justfile && git commit -m "feat: produce playground JSON events to Kafka"`.

### Task 3: Consumer, deduplication, and manual offset commits

**Files:**
- Modify: `src/ducklake_playground/engine.py`
- Create: `src/ducklake_playground/kafka_consumer.py`
- Modify: `tests/test_kafka_pipeline.py`

**Interfaces:**
- Consumes: Task 1 `decode_event` and `events_reader`; existing `DuckLakeEngine.merge_upsert`.
- Produces: `DuckLakeEngine.ensure_table(table_name: str, schema: pa.Schema) -> None`, `write_batch(engine: DuckLakeEngine, table_name: str, rows: Sequence[Mapping[str, object]], schema: pa.Schema) -> int`, and `consume(consumer: Consumer, engine: DuckLakeEngine, schema: pa.Schema, *, table_name: str, batch_size: int, batch_timeout: float, idle_exit: float | None) -> int`.

- [ ] **Step 1: Write failing consumer tests using a fake consumer and fake engine.** Cover a successful full batch (one write followed by one synchronous commit), a duplicate ID in a batch (one merge source row), mismatched Kafka key/JSON ID (no write or commit), malformed JSON (no write or commit), a write exception (no commit), and idle exit with a partial batch (write then commit). The fake consumer's `poll()` returns messages with `key()`, `value()`, and `error()` methods and then `None`.

  ```python
  consume(fake_consumer, fake_engine, schema, table_name="kafka_events", batch_size=2,
          batch_timeout=5.0, idle_exit=0.1)
  assert fake_engine.merged_ids == [expected_id]
  assert fake_consumer.commit_calls == [{"asynchronous": False}]
  ```
- [ ] **Step 2: Run `uv run pytest tests/test_kafka_pipeline.py -q`; expect failures for missing consumer behavior.**
- [ ] **Step 3: Add `ensure_table`.** In `engine.py`, check whether the qualified table exists with `SELECT 1 ... LIMIT 0`; on `duckdb.CatalogException`, call existing `_create_table` in a transaction. Do not drop or replace an existing table. Add a focused unit check using a fake connection or include the behavior in the Docker integration test.
- [ ] **Step 4: Implement batch processing.** In `kafka_consumer.py`, validate each message and key, decode with Task 1, keep the last row for each `id` within the batch, make a reader with `events_reader`, call `engine.ensure_table(table_name, schema)` then `engine.merge_upsert(table_name, reader, "id")`. Use a monotonic clock to flush when `batch_timeout` elapses even if the batch is not full. Disable Kafka auto commit and use `consumer.commit(asynchronous=False)` only after `write_batch` returns. On exceptions, close the consumer and engine but do not commit the pending batch. Flush a pending partial batch on idle exit or `KeyboardInterrupt` before closing.

  ```python
  latest_by_id = {int(row["id"]): row for row in rows}
  engine.ensure_table(table_name, schema)
  engine.merge_upsert(table_name, events_reader(list(latest_by_id.values()), schema), "id")
  consumer.commit(asynchronous=False)
  ```
- [ ] **Step 5: Add the consumer CLI.** `python -m ducklake_playground.kafka_consumer --batch-size 50 --batch-timeout 5 --idle-exit 10 --group-id ducklake-loader --topic events --table kafka_events` loads `config.yaml`, sets up the existing local DuckLake engine, creates a Kafka consumer with `enable.auto.commit=false` and `auto.offset.reset=earliest`, subscribes to the selected topic, then calls `consume`. The topic and table options allow isolated integration runs; their defaults are `events` and `kafka_events`.
- [ ] **Step 6: Run `uv run pytest tests/test_kafka_pipeline.py -q`; expect pass.** Run `uv run ruff check`, `uv run mypy src`, and `uv run pyright src`; repair only failures caused by this task.
- [ ] **Step 7: Commit:** `git add src/ducklake_playground/engine.py src/ducklake_playground/kafka_consumer.py tests/test_kafka_pipeline.py && git commit -m "feat: write Kafka batches to DuckLake with replay-safe merge"`.

### Task 4: Walkthrough and end-to-end verification

**Files:**
- Create: `docs/guides/kafka-streaming.md`, `tests/test_kafka_integration.py`
- Modify: `README.md`, `zensical.toml`, `justfile`

**Interfaces:**
- Consumes: Task 2 producer CLI and Task 3 consumer CLI.
- Produces: a reproducible local walkthrough, `just consume-events`, `just inspect-events`, and an integration test marked `integration`.

- [ ] **Step 1: Write a Docker-dependent integration test.** Start from a unique Kafka topic, consumer group, and DuckLake table name; run a finite producer set with deliberate duplicates, consume until idle, and query that table for `count(*)` and `count(distinct id)`. Record the count, replay the same topic with a new group, and assert both counts remain equal to the original unique-event count. Keep this test opt-in with `@pytest.mark.integration` and clean up its table without touching `demo_table` or the default `kafka_events` table.
- [ ] **Step 2: Run `uv run pytest tests/test_kafka_integration.py -m integration -q` with Postgres and Kafka running; investigate any pipeline failure before writing the walkthrough.**
- [ ] **Step 3: Add the walkthrough.** Explain the old Arrow streaming path versus this Kafka event path; show `just up-kafka`, separate producer/consumer commands, count/distinct-ID and snapshot queries, how to restart the same group, and how to replay with a new group. Include what a crash between DuckLake write and offset commit means, why `MERGE` handles exact duplicate events, how small writes may be inlined, and how to stop services. Add navigation from `README.md` and `zensical.toml`.
- [ ] **Step 4: Add `just` commands for consume and inspection.** Inspection should query the new table and show row count, distinct-ID count, and recent DuckLake snapshots. It must not modify data.

  ```sql
  SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids
  FROM playground_ducklake_local.main.kafka_events;
  SELECT * FROM playground_ducklake_local.snapshots()
  ORDER BY snapshot_id DESC LIMIT 10;
  ```
- [ ] **Step 5: Run the integration test, `uv run pytest tests -m 'not integration' -q`, `docker compose config`, and `git diff --check`; expect pass.** Manually follow the documented commands once, including a replay, and record the observed counts in the task report rather than hard-coding a transient value in the guide.
- [ ] **Step 6: Commit:** `git add docs/guides/kafka-streaming.md tests/test_kafka_integration.py README.md zensical.toml justfile && git commit -m "docs: teach Kafka replay into DuckLake"`.
