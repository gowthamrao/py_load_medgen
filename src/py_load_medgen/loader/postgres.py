import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, IO, List, Optional, Iterator

import psycopg

from py_load_medgen.loader.base import AbstractNativeLoader
from py_load_medgen.sql.ddl import (
    ETL_AUDIT_LOG_DDL,
    STAGING_CONCEPTS_DDL,
    STAGING_NAMES_DDL,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class PostgresNativeLoader(AbstractNativeLoader):
    """
    A native loader for PostgreSQL that uses the COPY protocol for high-performance
    data ingestion.
    """

    def __init__(
        self,
        db_dsn: Optional[str] = None,
        connection: Optional[psycopg.Connection] = None,
        autocommit: bool = True,
    ):
        """
        Initializes the PostgreSQL loader.
        Accepts either a DSN string or an existing connection object.
        Args:
            db_dsn: The database connection string (DSN).
            connection: An existing psycopg.Connection object (useful for testing).
            autocommit: If True, commits transactions automatically. Set to False
                        when an external transaction manager is used (e.g., in tests).
        """
        if not db_dsn and not connection:
            raise ValueError("Either db_dsn or connection must be provided.")

        self.dsn = db_dsn
        self.conn = connection
        self._managed_connection = connection is None
        self.autocommit = autocommit

    def _commit(self) -> None:
        """Commits the transaction if autocommit is enabled."""
        if self.autocommit and self.conn:
            self.conn.commit()

    def connect(self) -> None:
        """Establishes a connection and ensures metadata tables exist."""
        # Step 1: Establish connection if it doesn't exist or is closed.
        if not self.conn or self.conn.closed:
            if self._managed_connection and self.dsn:
                try:
                    logging.info("Connecting to PostgreSQL database...")
                    self.conn = psycopg.connect(self.dsn)
                    logging.info("Connection successful.")
                except psycopg.Error as e:
                    logging.error(f"Database connection error: {e}")
                    raise
            elif self.conn and self.conn.closed:
                raise ConnectionError(
                    "Managed connection was closed and cannot be reopened without a DSN."
                )
            elif not self._managed_connection:
                # This case means an external connection was provided but is closed.
                raise ConnectionError("The provided external connection is closed.")
            else:
                # This case means no DSN was provided for a managed connection.
                raise ConnectionError("Cannot connect without a DSN.")

        # Step 2: Initialize metadata on the now-active connection.
        self._initialize_metadata()

    def close(self) -> None:
        """Closes the database connection if it was created and is managed by this loader."""
        if self.conn and not self.conn.closed and self._managed_connection:
            self.conn.close()
            logging.info("Managed database connection closed.")
        else:
            logging.debug("Pre-existing connection not closed by loader.")

    def __enter__(self):
        """Context manager entry point, establishes connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, closes connection."""
        self.close()

    def initialize_staging(self, table_name: str, ddl: str) -> None:
        """
        Creates and prepares a single staging table for data loading.
        Args:
            table_name: The name of the staging table to create.
            ddl: The Data Definition Language (DDL) script to create the table.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Initializing staging table: {table_name}")
        with self.conn.cursor() as cur:
            with self.conn.transaction():
                cur.execute(ddl)
                cur.execute(f"TRUNCATE TABLE {table_name};")
        logging.info(f"Staging table {table_name} initialized successfully.")

    def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
        """
        Executes a native, high-performance bulk load operation using COPY.
        Args:
            table_name: The name of the target staging table.
            data_iterator: An iterator yielding bytes (e.g., TSV lines).
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Starting bulk load into '{table_name}'...")
        with self.conn.cursor() as cur:
            with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT TEXT, NULL '\\N')") as copy:
                for line in data_iterator:
                    copy.write(line)
        logging.info(f"Bulk load into '{table_name}' complete.")

    def _initialize_metadata(self) -> None:
        """Ensures the ETL audit log table exists."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info("Initializing metadata table: etl_audit_log")
        with self.conn.cursor() as cur:
            cur.execute(ETL_AUDIT_LOG_DDL)
        self._commit()
        logging.info("Metadata table initialized.")

    def log_run_start(
        self, run_id: uuid.UUID, package_version: str, load_mode: str, source_files: dict
    ) -> int:
        """Logs the start of an ETL run and returns the log_id."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        sql = """
            INSERT INTO etl_audit_log (run_id, package_version, load_mode, source_files, start_time, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING log_id;
        """
        start_time = datetime.now(timezone.utc)
        # Use psycopg's json adapter for safe serialization
        source_files_json = psycopg.types.json.Jsonb(source_files)

        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (run_id, package_version, load_mode, source_files_json, start_time, "In Progress"),
            )
            log_id = cur.fetchone()[0]
            self._commit()
        logging.info(f"ETL run started. Log ID: {log_id}")
        return log_id

    def log_run_finish(
        self,
        log_id: int,
        status: str,
        records_extracted: int,
        records_loaded: int,
        error_message: Optional[str] = None,
    ) -> None:
        """Logs the completion or failure of an ETL run."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        sql = """
            UPDATE etl_audit_log
            SET end_time = %s,
                status = %s,
                records_extracted = %s,
                records_loaded = %s,
                error_message = %s
            WHERE log_id = %s;
        """
        end_time = datetime.now(timezone.utc)

        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    end_time,
                    status,
                    records_extracted,
                    records_loaded,
                    error_message,
                    log_id,
                ),
            )
            self._commit()
        logging.info(f"ETL run finished for Log ID: {log_id}. Status: {status}")

    def _create_and_load_new_production_table(
        self, cur: psycopg.Cursor, new_production_table: str, staging_table: str, index_ddls: List[str]
    ) -> None:
        """Creates and loads data into a new production table and builds indexes."""
        logging.info(f"Creating new production table '{new_production_table}'...")

        cur.execute(f"CREATE TABLE {new_production_table} (LIKE {staging_table} INCLUDING ALL);")

        logging.info(f"Loading data from '{staging_table}' to '{new_production_table}'...")
        cur.execute(f"INSERT INTO {new_production_table} SELECT * FROM {staging_table};")

        cur.execute(f"ANALYZE {new_production_table};")

        logging.info(f"Creating indexes on '{new_production_table}'...")
        for index_ddl in index_ddls:
            # Substitute the placeholder with the actual new table name
            formatted_ddl = index_ddl.format(table_name=new_production_table)
            cur.execute(formatted_ddl)

        logging.info("New production table created and indexed successfully.")

    def execute_cdc(self) -> None:
        """Executes the Change Data Capture (CDC) logic (placeholder)."""
        logging.warning("`execute_cdc` is not yet implemented.")
        pass

    def apply_changes(self, staging_table: str, production_table: str, index_ddls: list[str]) -> None:
        """
        Applies the identified changes atomically using the 'atomic swap' method.
        Args:
            staging_table: The name of the staging table containing the new data.
            production_table: The name of the production table to be updated.
            index_ddls: A list of DDL statements to create indexes on the new production table.
                        Use a `{table_name}` placeholder for the table name.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        new_production_table = f"{production_table}_new"
        backup_table = f"{production_table}_old"

        logging.info(f"Applying changes for table {production_table} with atomic swap...")
        with self.conn.cursor() as cur:
            self._create_and_load_new_production_table(cur, new_production_table, staging_table, index_ddls)

            logging.info("Performing atomic swap in a single transaction...")
            with self.conn.transaction():
                cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
                cur.execute(f"ALTER TABLE IF EXISTS {production_table} RENAME TO {backup_table};")
                cur.execute(f"ALTER TABLE {new_production_table} RENAME TO {production_table};")

        logging.info(f"Atomic swap complete for {production_table}. Production data is updated.")

    def cleanup(self, staging_table: str, production_table: str) -> None:
        """
        Performs cleanup operations by dropping the old backup and staging tables.
        Args:
            staging_table: The name of the staging table to drop.
            production_table: The name of the production table, used to derive the backup table name.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        backup_table = f"{production_table}_old"

        logging.info(f"Performing cleanup for {production_table}...")
        with self.conn.cursor() as cur:
            with self.conn.transaction():
                cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
                cur.execute(f"DROP TABLE IF EXISTS {staging_table} CASCADE;")
        logging.info(f"Cleanup complete. Dropped tables: {backup_table}, {staging_table}")
