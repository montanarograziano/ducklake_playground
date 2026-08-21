"""DuckLake engine using DuckDB + PostgreSQL metadata catalog.

Writes consume a registered ``pa.RecordBatchReader`` via ``INSERT INTO ... SELECT * FROM <reader>``
to stream batches without materializing the full dataset. Tables are partitioned by
``event_date`` so DuckLake emits one Parquet file per partition.

Lifecycle: ``setup`` → many ``write_*`` / ``read_*`` / SQL ops via ``con`` → ``teardown``
(optional, drops the table) → ``close``.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
from loguru import logger

from .data_generator import PARTITION_COL

if TYPE_CHECKING:
    from .config import PlaygroundConfig

_MIN_DUCKDB_VERSION = "1.5.4"
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
        created in PostgreSQL on first call via ``psycopg``.

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
        if self._version_tuple(version) < self._version_tuple(_MIN_DUCKDB_VERSION):
            msg = f"DuckDB {_MIN_DUCKDB_VERSION}+ required, got {version}"
            raise RuntimeError(msg)

        con = duckdb.connect()
        self._con = con
        con.execute("INSTALL ducklake; INSTALL postgres;")
        con.execute("LOAD ducklake; LOAD postgres;")
        # Allow the writer to reorder rows for better file packing.
        con.execute("PRAGMA preserve_insertion_order = false;")

        pg = config.postgres
        if storage_mode == "s3":
            s3 = config.s3
            self._data_path = f"s3://{s3.bucket}/{s3.ducklake_prefix}"
            endpoint_stripped = s3.endpoint.replace("http://", "").replace("https://", "")
            con.execute(f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE S3,
                    KEY_ID {self._sql_string(s3.access_key)},
                    SECRET {self._sql_string(s3.secret_key)},
                    ENDPOINT {self._sql_string(endpoint_stripped)},
                    URL_STYLE 'path',
                    USE_SSL false
                );
            """)
        else:
            base = os.path.abspath(config.local.base_path)
            self._data_path = os.path.join(base, config.local.ducklake_prefix)
            try:
                os.makedirs(self._data_path, exist_ok=True)
            except OSError as exc:
                msg = f"Could not create DuckLake data directory: {self._data_path}"
                raise RuntimeError(msg) from exc

        self._pg_database = f"{pg.database}_{storage_mode}"
        self._ensure_postgres_db(pg.host, pg.port, pg.user, pg.password, self._pg_database)

        con.execute(f"""
            CREATE OR REPLACE SECRET postgres_secret (
                TYPE postgres,
                HOST {self._sql_string(pg.host)},
                PORT {pg.port},
                DATABASE {self._sql_string(self._pg_database)},
                USER {self._sql_string(pg.user)},
                PASSWORD {self._sql_string(pg.password)}
            );
        """)
        # Attach the catalog DB for postgres_query (used by get_postgres_metadata_size).
        self._pg_attach_name = f"pg_meta_{storage_mode}"
        try:
            con.execute(
                f"ATTACH '' AS {self._quote_identifier(self._pg_attach_name)} (TYPE POSTGRES, SECRET postgres_secret);"
            )
        except Exception as exc:  # pragma: no cover
            logger.warning(f"Could not ATTACH Postgres catalog for size measurement: {exc}")

        attach_uri = (
            f"ducklake:postgres:dbname={self._pg_database} host={pg.host} "
            f"port={pg.port} user={pg.user} password={pg.password}"
        )
        con.execute(f"""
            ATTACH OR REPLACE {self._sql_string(attach_uri)} AS {self._quote_identifier(self._catalog_name)}
                (DATA_PATH {self._sql_string(self._data_path)});
        """)
        con.execute(f"USE {self._quote_identifier(self._catalog_name)};")

        self._pg_baseline_bytes = self._query_pg_database_size()

        logger.info(
            f"DuckLake engine setup complete (storage={storage_mode}, "
            f"data_path={self._data_path}, "
            f"pg_baseline={self._pg_baseline_bytes / 1024:.1f} KB)"
        )

    def teardown(self, table_name: str) -> None:
        """Drop only the named table.

        This deliberately does not delete the shared ``DATA_PATH``. A catalog can contain
        multiple tables, and physical cleanup must use DuckLake's snapshot expiry and
        cleanup procedures after choosing an appropriate retention policy.
        """
        if self._con is None:
            return
        fq = self._qualified(table_name)
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
        except Exception as exc:
            logger.warning(f"Failed to drop table {fq}")
            raise RuntimeError(f"Could not drop table {table_name!r}") from exc

    def close(self) -> None:
        """Close the DuckDB connection and detach catalogs. Idempotent."""
        if self._con is not None:
            try:
                self._con.execute("USE memory;")
                self._con.execute(f"DETACH IF EXISTS {self._quote_identifier(self._catalog_name)};")
                if self._pg_attach_name:
                    self._con.execute(f"DETACH IF EXISTS {self._quote_identifier(self._pg_attach_name)};")
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
        self._con.execute("BEGIN TRANSACTION")
        try:
            try:
                self._con.execute(f"SELECT 1 FROM {fq} LIMIT 0")
            except duckdb.CatalogException:
                self._create_table(fq, schema)
            self._insert_reader(fq, reader)
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

    def write_overwrite(
        self,
        table_name: str,
        reader: pa.RecordBatchReader,
        schema: pa.Schema,
    ) -> None:
        """Drop and recreate the table with new data. One DuckLake snapshot per call."""
        assert self._con is not None
        fq = self._qualified(table_name)
        self._con.execute("BEGIN TRANSACTION")
        try:
            self._con.execute(f"DROP TABLE IF EXISTS {fq}")
            self._create_table(fq, schema)
            self._insert_reader(fq, reader)
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

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
        if merge_key not in source_reader.schema.names:
            raise ValueError(f"merge_key {merge_key!r} is not present in the source schema")
        non_key_cols = [name for name in source_reader.schema.names if name != merge_key]
        if not non_key_cols:
            raise ValueError("MERGE requires at least one non-key source column to update")
        self._con.register("_merge_src", source_reader)
        set_clause = ", ".join(
            f"{self._quote_identifier(c)} = source.{self._quote_identifier(c)}" for c in non_key_cols
        )
        insert_cols = ", ".join(self._quote_identifier(c) for c in source_reader.schema.names)
        insert_vals = ", ".join(f"source.{self._quote_identifier(c)}" for c in source_reader.schema.names)
        sql = f"""
            MERGE INTO {fq} AS target
            USING _merge_src AS source
            ON target.{self._quote_identifier(merge_key)} = source.{self._quote_identifier(merge_key)}
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
        assert self._con is not None
        result = self._con.execute(
            "SELECT COALESCE(SUM(data_file_size_bytes), 0), COUNT(*) "
            f"FROM ducklake_list_files({self._sql_string(self._catalog_name)}, {self._sql_string(table_name)})"
        ).fetchone()
        assert result is not None
        try:
            return int(result[0]), int(result[1])
        except (TypeError, ValueError) as exc:
            msg = f"Unexpected ducklake_list_files result: {result!r}"
            raise RuntimeError(msg) from exc

    def get_catalog_metadata_size(self) -> int:
        """Return the entire catalog DB's growth since engine setup (bytes).

        This is catalog-scoped, not table-scoped: PostgreSQL accounts for shared indexes,
        free space, and all DuckLake tables together.
        """
        current = self._query_pg_database_size()
        delta = current - self._pg_baseline_bytes
        return max(delta, 0)

    def get_postgres_metadata_size(self, table_name: str) -> int:
        """Deprecated compatibility wrapper for :meth:`get_catalog_metadata_size`.

        ``table_name`` is ignored because PostgreSQL cannot report reliable per-table
        catalog growth with this measurement method.
        """
        _ = table_name
        return self.get_catalog_metadata_size()

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

    def qualified_table(self, table_name: str) -> str:
        """Return a safely quoted fully qualified table name for ad-hoc SQL."""
        return self._qualified(table_name)

    # ────────────────────────────────────────────────────────────────────
    # Internals
    # ────────────────────────────────────────────────────────────────────

    def _qualified(self, table_name: str) -> str:
        return ".".join((self._quote_identifier(self._catalog_name), "main", self._quote_identifier(table_name)))

    @staticmethod
    def _quote_identifier(value: str) -> str:
        """Return a SQL identifier quoted for DuckDB."""
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _sql_string(value: str) -> str:
        """Return a SQL string literal with single quotes escaped."""
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _version_tuple(version: str) -> tuple[int, ...]:
        """Compare dotted numeric versions without lexicographic errors (e.g. 1.10 > 1.5)."""
        try:
            return tuple(int(part) for part in version.split(".") if part.isdigit())
        except ValueError as exc:  # pragma: no cover - guarded by isdigit
            msg = f"Invalid DuckDB version: {version!r}"
            raise RuntimeError(msg) from exc

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
                "SELECT * FROM postgres_query("
                f"{self._sql_string(self._pg_attach_name)}, 'SELECT pg_database_size(current_database())')"
            ).fetchone()
            return int(row[0]) if row is not None else 0
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning(f"Could not query pg_database_size: {exc}")
            return 0

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
            self._con.execute(f"ALTER TABLE {fq} SET PARTITIONED BY ({self._quote_identifier(PARTITION_COL)})")
        except Exception as exc:
            raise RuntimeError(f"Could not partition {fq} by {PARTITION_COL}") from exc

    def _insert_reader(self, fq: str, reader: pa.RecordBatchReader) -> None:
        """Stream a RecordBatchReader into ``fq`` via INSERT ... SELECT * FROM <registered>."""
        assert self._con is not None
        self._con.register("_arrow_src", reader)
        try:
            self._con.execute(f"INSERT INTO {fq} SELECT * FROM _arrow_src")
        finally:
            self._con.unregister("_arrow_src")
