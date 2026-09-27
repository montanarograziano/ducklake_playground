"""Opt-in test of Kafka delivery, DuckLake writes, and replay."""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from ducklake_playground import DuckLakeEngine, load_config

REPO_ROOT = Path(__file__).parents[1]


def _run(*args: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=90, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.integration
def test_replay_does_not_add_duplicate_rows() -> None:
    suffix = uuid.uuid4().hex[:12]
    topic = f"playground-events-{suffix}"
    table = f"kafka_events_test_{suffix}"
    group = f"ducklake-test-{suffix}"
    _run("ducklake_playground.kafka_producer", "--count", "12", "--duplicate-every", "4", "--topic", topic)

    engine = DuckLakeEngine()
    engine.setup(load_config(REPO_ROOT / "config.yaml"), "local")
    try:
        for replay in range(2):
            _run(
                "ducklake_playground.kafka_consumer",
                "--topic",
                topic,
                "--table",
                table,
                "--group-id",
                f"{group}-{replay}",
                "--batch-size",
                "5",
                "--batch-timeout",
                "0.5",
                "--idle-exit",
                "8",
            )
            row = engine.connection.execute(
                f"SELECT count(*), count(DISTINCT id) FROM {engine.qualified_table(table)}"
            ).fetchone()
            assert row == (12, 12)
    finally:
        engine.teardown(table)
        engine.close()
