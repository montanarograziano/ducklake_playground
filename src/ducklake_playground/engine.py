"""DuckLake engine using DuckDB + PostgreSQL metadata catalog.

Writes consume a registered ``pa.RecordBatchReader`` via ``INSERT INTO ... SELECT * FROM <reader>``
to stream batches without materializing the full dataset. Tables are partitioned by
``event_date`` so DuckLake emits one Parquet file per partition.

Lifecycle: ``setup`` → many ``write_*`` / ``read_*`` / SQL ops via ``con`` → ``teardown``
(optional, drops the table) → ``close``.
"""

from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
from loguru import logger

from .data_generator import PARTITION_COL

if TYPE_CHECKING:
    from .config import PlaygroundConfig

_MIN_DUCKDB_VERSION = "1.5.2"
"""Minimum DuckDB version required by the DuckLake extension."""


class DuckLakeEngine:
    """DuckLake engine for the playground.

    Wraps DuckDB's ``ducklake`` extension with a PostgreSQL metadata catalog and either a
    local filesystem or S3-compatible (MinIO) data backend. Tables are partitioned by
    ``event_date``; writes consume a ``pa.RecordBatchReader`` so memory is bounded by
    ``chunk_size`` rather than total dataset size.

    The DuckDB connection is exposed as ``self._con`` for ad-hoc SQL from notebooks; pass it
    to ``mo.sql(..., engine=self._con)`` in marimo.
    """

    name: str = "ducklake"

    def __init__(self) -> None:
        """Construct an unattached engine. Call :meth:`setup` to attach a catalog."""
        self._con: duckdb.DuckDBPyConnection | None = None
        self._config: PlaygroundConfig | None = None
        self._storage_mode: str = ""
        self._data_path: str = ""
        self._catalog_name: str = ""
        self._pg_database: str = ""
        self._pg_baseline_bytes: int = 0
        self._pg_attach_name: str = ""

    # ────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ────────────────────────────────────────────────────────────────────

    def setup(self, config: PlaygroundConfig, storage_mode: str) -> None:
        """Initialize the DuckDB connection, install extensions, attach DuckLake.

        Idempotent on repeat calls (uses ``ATTACH OR REPLACE``). The catalog database is
        created in PostgreSQL on first call via ``psql``.

        Args:
            config: Playground configuration (typically from :func:`load_config`).
            storage_mode: ``"local"`` or ``"s3"``. Selects which storage backend in
                ``config`` to use and namespaces the catalog accordingly.

        Raises:
            RuntimeError: If the installed DuckDB version is older than
                :data:`_MIN_DUCKDB_VERSION` (the minimum supporting DuckLake).
        """
        self._config = config
        self._storage_mode = storage_mode
        self._catalog_name = f"playground_ducklake_{storage_mode}"

        version = duckdb.__version__
        if version < _MIN_DUCKDB_VERSION:
            msg = f"DuckDB {_MIN_DUCKDB_VERSION}+ required, got {version}"
            raise RuntimeError(msg)

        self._con = duckdb.connect()
        self._con.execute("INSTALL ducklake; INSTALL postgres;")
        self._con.execute("LOAD ducklake; LOAD postgres;")
        # Allow the writer to reorder rows for better file packing.
        self._con.execute("PRAGMA preserve_insertion_order = false;")

        pg = config.postgres
        if storage_mode == "s3":
            s3 = config.s3
            self._data_path = f"s3://{s3.bucket}/{s3.ducklake_prefix}"
            endpoint_stripped = s3.endpoint.replace("http://", "").replace("https://", "")
            self._con.execute(f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE S3,
                    KEY_ID '{s3.access_key}',
                    SECRET '{s3.secret_key}',
                    ENDPOINT '{endpoint_stripped}',
                    URL_STYLE 'path',
                    USE_SSL false
                );
            """)
        else:
            base = os.path.abspath(config.local.base_path)
            self._data_path = os.path.join(base, config.local.ducklake_prefix)
            os.makedirs(self._data_path, exist_ok=True)

        self._pg_database = f"{pg.database}_{storage_mode}"
        self._ensure_postgres_db(pg.host, pg.port, pg.user, pg.password, self._pg_database)

        self._con.execute(f"""
            CREATE OR REPLACE SECRET postgres_secret (
                TYPE postgres,
                HOST '{pg.host}',
                PORT {pg.port},
                DATABASE '{self._pg_database}',
                USER '{pg.user}',
                PASSWORD '{pg.password}'
            );
        """)
        # Attach the catalog DB for postgres_query (used by get_postgres_metadata_size).
        self._pg_attach_name = f"pg_meta_{storage_mode}"
        try:
            self._con.execute(f"ATTACH '' AS {self._pg_attach_name} (TYPE POSTGRES, SECRET postgres_secret);")
        except Exception as exc:  # pragma: no cover
            logger.warning(f"Could not ATTACH Postgres catalog for size measurement: {exc}")

        attach_uri = (
            f"ducklake:postgres:dbname={self._pg_database} host={pg.host} "
            f"port={pg.port} user={pg.user} password={pg.password}"
        )
        self._con.execute(f"""
            ATTACH OR REPLACE '{attach_uri}' AS {self._catalog_name}
                (DATA_PATH '{self._data_path}');
        """)
        self._con.execute(f"USE {self._catalog_name};")

        self._pg_baseline_bytes = self._query_pg_database_size()

        logger.info(
            f"DuckLake engine setup complete (storage={storage_mode}, "
            f"data_path={self._data_path}, "
            f"pg_baseline={self._pg_baseline_bytes / 1024:.1f} KB)"
        )

    def teardown(self, table_name: str) -> None:
        """Drop the named table and clean up its data files.

        Removes the entire ``data_path`` directory on local mode and deletes the S3 prefix
        on S3 mode. The Postgres metadata is left intact (catalog still attached); call
        :meth:`close` to detach.
        """
        if self._con is None:
            return
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        except Exception:
            logger.warning(f"Failed to drop table {fq}")

        if self._storage_mode == "local" and self._data_path and os.path.exists(self._data_path):
            shutil.rmtree(self._data_path, ignore_errors=True)
            os.makedirs(self._data_path, exist_ok=True)
        elif self._storage_mode == "s3" and self._config is not None:
            self._cleanup_s3_files()

    def close(self) -> None:
        """Close the DuckDB connection and detach catalogs. Idempotent."""
        if self._con is not None:
            try:
                self._con.execute("USE memory;")
                self._con.execute(f"DETACH IF EXISTS {self._catalog_name};")
                if self._pg_attach_name:
                    self._con.execute(f"DETACH IF EXISTS {self._pg_attach_name};")
            except Exception as exc:
                logger.debug(f"close() detach warning: {exc}")
            self._con.close()
            self._con = None

    # ────────────────────────────────────────────────────────────────────
    # Writes
    # ────────────────────────────────────────────────────────────────────

    def write_append(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Append data, creating the table on first call. One DuckLake snapshot per call."""
        assert self._con is not None
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"SELECT 1 FROM {fq} LIMIT 0")
        except duckdb.CatalogException:
            self._create_table(fq, schema)
        self._insert_reader(fq, reader)

    def write_overwrite(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Drop and recreate the table with new data. One DuckLake snapshot per call."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        self._create_table(fq, schema)
        self._insert_reader(fq, reader)

    def merge_upsert(
        self,
        table_name: str,
        source_reader: pa.RecordBatchReader,
        merge_key: str,
    ) -> None:
        """``MERGE INTO`` upsert. Streams source via the registered Arrow reader.

        Updates rows where ``target.{merge_key} = source.{merge_key}``, inserts non-matching
        source rows. All non-key columns are updated.
        """
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.register("_merge_src", source_reader)
        non_key_cols = [name for name in source_reader.schema.names if name != merge_key]
        set_clause = ", ".join(f"{c} = source.{c}" for c in non_key_cols)
        insert_cols = ", ".join(source_reader.schema.names)
        insert_vals = ", ".join(f"source.{c}" for c in source_reader.schema.names)
        sql = f"""
            MERGE INTO {fq} AS target
            USING _merge_src AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        try:
            self._con.execute(sql)
        finally:
            self._con.unregister("_merge_src")

    # ────────────────────────────────────────────────────────────────────
    # Reads
    # ────────────────────────────────────────────────────────────────────

    def read_full_scan(self, table_name: str) -> int:
        """Streamed full scan. Returns row count without materializing the result.

        Iterates the Arrow record batch reader and counts rows. This forces DuckDB to read
        and decode every Parquet file and every column, but bounds reader memory by one
        batch at a time. Compare to ``.fetch_arrow_table()`` which materializes the entire
        result in Python.
        """
        assert self._con is not None
        fq = self._qualified(table_name)
        reader = self._con.execute(f"SELECT * FROM {fq}").fetch_record_batch()
        total = 0
        for batch in reader:
            total += batch.num_rows
        return total

    def read_aggregation(self, table_name: str) -> pa.Table:
        """``GROUP BY varchar_col`` aggregation. Returns an Arrow table (small)."""
        assert self._con is not None
        fq = self._qualified(table_name)
        sql = f"""
            SELECT varchar_col,
                   COUNT(*) AS cnt,
                   SUM(int64_col) AS sum_val,
                   AVG(float64_col) AS avg_val,
                   MIN({PARTITION_COL}) AS min_date,
                   MAX({PARTITION_COL}) AS max_date
            FROM {fq}
            GROUP BY varchar_col
        """  # noqa: S608
        return self._con.execute(sql).fetch_arrow_table()

    # ────────────────────────────────────────────────────────────────────
    # Introspection
    # ────────────────────────────────────────────────────────────────────

    def get_disk_usage(self, table_name: str) -> tuple[int, int]:
        """Return ``(total_bytes, file_count)`` for the table's data files."""
        assert self._config is not None
        if self._storage_mode == "s3":
            from .metrics import get_s3_disk_usage

            s3 = self._config.s3
            return get_s3_disk_usage(
                bucket=s3.bucket,
                prefix=s3.ducklake_prefix,
                endpoint=s3.endpoint,
                access_key=s3.access_key,
                secret_key=s3.secret_key,
            )
        from .metrics import get_local_disk_usage

        return get_local_disk_usage(self._data_path)

    def get_postgres_metadata_size(self, table_name: str) -> int:
        """Return current ``pg_database_size`` minus the empty-catalog baseline (bytes).

        Approximation only: measures the entire DuckLake catalog DB, not just this table.
        Adequate for "metadata adds X MB on top of data files" claims; not for sub-MB
        precision.
        """
        current = self._query_pg_database_size()
        delta = current - self._pg_baseline_bytes
        return max(delta, 0)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The underlying DuckDB connection.

        Pass to ``mo.sql(..., engine=engine.connection)`` in marimo notebooks, or use
        ``engine.connection.execute(...)`` for ad-hoc Python queries. Raises if the engine
        has not been :meth:`setup`.
        """
        if self._con is None:
            msg = "Engine is not attached. Call setup() first."
            raise RuntimeError(msg)
        return self._con

    @property
    def catalog_name(self) -> str:
        """The DuckLake catalog name attached by :meth:`setup`."""
        return self._catalog_name

    @property
    def data_path(self) -> str:
        """Resolved data path (local absolute path or ``s3://...`` URI)."""
        return self._data_path

    # ────────────────────────────────────────────────────────────────────
    # Internals
    # ────────────────────────────────────────────────────────────────────

    def _qualified(self, table_name: str) -> str:
        return f"{self._catalog_name}.main.{table_name}"

    def _ensure_postgres_db(self, host: str, port: int, user: str, password: str, database: str) -> None:
        """Create the catalog database in PostgreSQL if it does not exist.

        Uses ``psycopg`` in autocommit mode against the maintenance ``postgres`` database.
        DuckDB's ``postgres_execute`` wraps statements in a transaction, which Postgres
        rejects for ``CREATE DATABASE``, so the extension cannot be used here.
        """
        import psycopg
        from psycopg import sql

        with (
            psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname="postgres",
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if cur.fetchone() is not None:
                logger.debug(f"Database {database} already exists")
                return
            # sql.Identifier already produces the safely-quoted identifier
            # ("foo"); wrapping it again in "..." would double the quotes and
            # trigger Postgres's "zero-length delimited identifier" error.
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            logger.info(f"Created PostgreSQL database: {database}")

    def _query_pg_database_size(self) -> int:
        """Return ``pg_database_size`` for the catalog DB via DuckDB's postgres_query."""
        assert self._con is not None
        if not self._pg_attach_name:
            return 0
        try:
            row = self._con.execute(
                f"SELECT * FROM postgres_query('{self._pg_attach_name}', 'SELECT pg_database_size(current_database())')"
            ).fetchone()
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning(f"Could not query pg_database_size: {exc}")
            return 0
        if row is None:
            return 0
        return int(row[0])

    def _create_table(self, fq: str, schema: pa.Schema) -> None:
        """Create the DuckLake table with the given Arrow schema, partitioned by event_date."""
        assert self._con is not None
        empty = pa.Table.from_pylist([], schema=schema)
        self._con.register("_arrow_empty", empty)
        try:
            self._con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _arrow_empty WHERE 0=1")
        finally:
            self._con.unregister("_arrow_empty")
        try:
            self._con.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({PARTITION_COL})")
        except Exception as exc:
            logger.warning(f"Could not set partitioning via ALTER (will fall back to unpartitioned): {exc}")

    def _insert_reader(self, fq: str, reader: pa.RecordBatchReader) -> None:
        """Stream a RecordBatchReader into ``fq`` via INSERT ... SELECT * FROM <registered>."""
        assert self._con is not None
        self._con.register("_arrow_src", reader)
        try:
            self._con.execute(f"INSERT INTO {fq} SELECT * FROM _arrow_src")
        finally:
            self._con.unregister("_arrow_src")

    def _cleanup_s3_files(self) -> None:
        """Remove data files from S3/MinIO using botocore."""
        from .metrics import s3_rm_recursive

        assert self._config is not None
        s3 = self._config.s3
        prefix = s3.ducklake_prefix
        try:
            n = s3_rm_recursive(
                bucket=s3.bucket,
                prefix=prefix,
                endpoint=s3.endpoint,
                access_key=s3.access_key,
                secret_key=s3.secret_key,
            )
            logger.debug(f"Cleaned up {n} S3 objects at s3://{s3.bucket}/{prefix}")
        except Exception:
            logger.warning(f"Failed to clean up S3 path: s3://{s3.bucket}/{prefix}")
