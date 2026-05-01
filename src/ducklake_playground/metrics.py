"""Metric collection utilities.

Memory is measured via process RSS sampled in a background thread. This captures C++/Rust
allocations from DuckDB and Polars (which ``tracemalloc`` does not see). RSS includes
mmap-backed file pages, so for read-heavy workloads it can include OS-level page cache.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psutil
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class TimingResult:
    """Wall time + RSS sampled during a measured block.

    Attributes:
        wall_time_seconds: Elapsed time (``time.perf_counter``).
        peak_rss_mb: Absolute peak process RSS during the measured block.
        delta_rss_mb: ``peak_rss_mb`` minus baseline RSS at block entry. The "true cost"
            of the operation, baseline-subtracted.
    """

    wall_time_seconds: float
    peak_rss_mb: float
    delta_rss_mb: float


@contextmanager
def measure_time_and_memory(sample_interval_s: float = 0.05) -> Iterator[list[TimingResult]]:
    """Measure wall time and peak process RSS over the wrapped block.

    Args:
        sample_interval_s: How often the background thread samples RSS. Tighten for
            sub-100ms operations; default 50 ms is fine for most workloads.

    Yields:
        A single-element list which gets populated with one ``TimingResult`` on exit.

    Example:
        >>> with measure_time_and_memory() as t:
        ...     # do work
        ...     pass
        >>> result = t[0]
        >>> print(result.wall_time_seconds, result.peak_rss_mb, result.delta_rss_mb)
    """
    proc = psutil.Process(os.getpid())
    baseline = proc.memory_info().rss
    peak = baseline
    stop_evt = threading.Event()

    def _sample() -> None:
        nonlocal peak
        while not stop_evt.is_set():
            try:
                rss = proc.memory_info().rss
            except psutil.NoSuchProcess:
                return
            if rss > peak:
                peak = rss
            stop_evt.wait(sample_interval_s)

    container: list[TimingResult] = []
    sampler = threading.Thread(target=_sample, daemon=True)
    start = time.perf_counter()
    sampler.start()
    try:
        yield container
    finally:
        elapsed = time.perf_counter() - start
        stop_evt.set()
        sampler.join()
        container.append(
            TimingResult(
                wall_time_seconds=elapsed,
                peak_rss_mb=peak / (1024 * 1024),
                delta_rss_mb=(peak - baseline) / (1024 * 1024),
            )
        )
        logger.debug(
            f"Elapsed: {elapsed:.3f}s | Peak RSS: {peak / (1024 * 1024):.1f} MB "
            f"| Delta RSS: {(peak - baseline) / (1024 * 1024):.1f} MB"
        )


def get_local_disk_usage(path: str) -> tuple[int, int]:
    """Return total bytes and file count for a local directory tree.

    Args:
        path: Directory path to measure.

    Returns:
        ``(total_bytes, file_count)``. Both ``0`` if path does not exist.
    """
    total_bytes = 0
    file_count = 0
    target = os.path.abspath(path)
    if not os.path.exists(target):
        return 0, 0
    for dirpath, _dirnames, filenames in os.walk(target):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_bytes += os.path.getsize(fp)
                file_count += 1
    return total_bytes, file_count


def get_s3_disk_usage(*, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str) -> tuple[int, int]:
    """Return total bytes and file count for an S3 prefix.

    Uses DuckDB's ``httpfs`` extension via ``glob()`` to enumerate keys.

    Returns:
        ``(total_bytes, file_count)``. Both ``0`` on failure.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE SECRET s3_measure (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint.replace("http://", "").replace("https://", "")}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    s3_path = f"s3://{bucket}/{prefix}**"
    try:
        result = con.execute(f"""
            SELECT COALESCE(SUM(size), 0) AS total_bytes, COUNT(*) AS file_count
            FROM glob('{s3_path}')
        """).fetchone()
        con.close()
        if result:
            return int(result[0]), int(result[1])
    except Exception:
        logger.warning(f"Could not measure S3 disk usage for {s3_path}")
        con.close()
    return 0, 0


def _get_s3_client(*, endpoint: str, access_key: str, secret_key: str):  # noqa: ANN202
    """Create a botocore S3 client with explicit credentials (no env/profile leakage)."""
    import botocore.session  # type: ignore[import-untyped]

    session = botocore.session.get_session()
    return session.create_client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def s3_rm_recursive(*, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str) -> int:
    """Delete all objects under an S3 prefix. Returns count deleted."""
    client = _get_s3_client(endpoint=endpoint, access_key=access_key, secret_key=secret_key)
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_request = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        client.delete_objects(Bucket=bucket, Delete=delete_request)
        deleted += len(objects)
    return deleted
