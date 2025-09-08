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
        """Commits the transaction if a connection exists."""
        if self.conn and not self.conn.closed:
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
        self._commit()
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
        self._commit()
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

    def execute_cdc(
        self, staging_table: str, production_table: str, pk_name: str, business_key: str
    ) -> dict[str, int]:
        """
        Executes Change Data Capture (CDC) logic using SQL.
        This method identifies inserts, updates, and deletes by comparing the
        staging table to the production table. For simplicity in this implementation,
        updates are treated as a soft-delete of the old record and an insert of the new one.
        Args:
            staging_table: The name of the staging table.
            production_table: The name of the production table.
            pk_name: The name of the primary key column of the production table.
            business_key: The column name of the business key used for comparison.
        Returns:
            A dictionary with counts of records to be inserted and deleted.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Executing CDC for {production_table} using key '{business_key}'...")
        with self.conn.cursor() as cur:
            # Create temporary tables to store the IDs of records to be changed
            cur.execute("CREATE TEMP TABLE cdc_deletes (id BIGINT) ON COMMIT DROP;")
            # Create a generic temp table for inserts based on the staging table's structure
            cur.execute(f"CREATE TEMP TABLE cdc_inserts (LIKE {staging_table} INCLUDING DEFAULTS) ON COMMIT DROP;")

            # --- Identify Deletes ---
            # Find records in production that are NOT in the new staging data.
            # These will be marked as inactive (soft-deleted).
            sql_find_deletes = f"""
                INSERT INTO cdc_deletes (id)
                SELECT p.{pk_name}
                FROM {production_table} p
                LEFT JOIN {staging_table} s ON p.{business_key} = s.{business_key}
                WHERE s.{business_key} IS NULL AND p.is_active = true;
            """
            cur.execute(sql_find_deletes)
            delete_count = cur.rowcount

            # --- Identify Inserts ---
            # Find records in staging that are NOT in the current active production data.
            sql_find_inserts = f"""
                INSERT INTO cdc_inserts
                SELECT s.*
                FROM {staging_table} s
                LEFT JOIN {production_table} p ON s.{business_key} = p.{business_key} AND p.is_active = true
                WHERE p.{pk_name} IS NULL;
            """
            cur.execute(sql_find_inserts)
            insert_count = cur.rowcount

        logging.info(f"CDC complete. Inserts: {insert_count}, Deletes: {delete_count}")
        return {"inserts": insert_count, "deletes": delete_count}

    def _get_table_indexes(self, table_name: str) -> list[str]:
        """
        Retrieves the DDL for all non-primary-key indexes on a given table.
        Args:
            table_name: The name of the table to inspect.
        Returns:
            A list of `CREATE INDEX` statements.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Discovering indexes for table: {table_name}")
        # This query joins pg_indexes with pg_class to filter out primary key
        # indexes, which are handled automatically by the table DDL.
        sql = """
            SELECT indexdef
            FROM pg_indexes i
            JOIN pg_class c ON i.indexname = c.relname
            LEFT JOIN pg_constraint con ON c.oid = con.conindid
            WHERE i.tablename = %s AND con.contype IS DISTINCT FROM 'p';
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, (table_name,))
                # The query returns a list of tuples, e.g., [('CREATE INDEX...',)]
                index_ddls = [row[0] for row in cur.fetchall()]
                logging.info(f"Found {len(index_ddls)} non-PK indexes for {table_name}.")
                return index_ddls
            except psycopg.errors.UndefinedTable:
                logging.warning(f"Table '{table_name}' does not exist, cannot discover indexes. Returning empty list.")
                return []

    def apply_changes(
        self,
        mode: str,
        staging_table: str,
        production_table: str,
        production_ddl: str,
        pk_name: str,
        business_key: str,
    ) -> None:
        """
        Applies changes to the production table based on the load mode.
        - 'full': Performs an atomic swap (rename and replace).
        - 'delta': Applies inserts and soft deletes identified by CDC.
        """
        if mode == "full":
            self._apply_full_load(staging_table, production_table, production_ddl)
        elif mode == "delta":
            self._apply_delta_load(production_table, pk_name)
        else:
            raise ValueError(f"Unknown load mode: {mode}")

    def _apply_full_load(
        self, staging_table: str, production_table: str, production_ddl: str
    ) -> None:
        """
        Applies changes atomically using the 'atomic swap' method for a full load.
        It automatically discovers and replicates indexes from the old production table.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        new_production_table = f"{production_table}_new"
        backup_table = f"{production_table}_old"

        # Discover indexes on the current production table before doing anything else.
        # This handles the initial case where the production table might not exist yet.
        index_ddls = self._get_table_indexes(production_table)

        logging.info(f"Applying FULL load for table {production_table} with atomic swap...")
        with self.conn.cursor() as cur:
            # Create the new production table using the explicit DDL
            cur.execute(production_ddl.format(table_name=new_production_table))

            # Dynamically build the column list from the staging table to make the INSERT generic
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position;",
                (staging_table,),
            )
            columns = [row[0] for row in cur.fetchall()]
            column_list_str = ", ".join(columns)

            # Load data from staging
            logging.info(f"Loading data from '{staging_table}' into '{new_production_table}'")
            insert_sql = f"INSERT INTO {new_production_table} ({column_list_str}) SELECT {column_list_str} FROM {staging_table};"
            cur.execute(insert_sql)

            # Create indexes on the new table. The discovered index DDL contains the old
            # table name, so we must replace it with the new table name.
            logging.info(f"Replicating {len(index_ddls)} indexes on new table {new_production_table}...")
            for index_ddl in index_ddls:
                # Use a more precise replacement to avoid incorrectly renaming the index itself
                # if the table name is part of the index name.
                prefix, sep, suffix = index_ddl.rpartition(f" ON {production_table}")
                if sep:
                    replicated_ddl = prefix + f" ON {new_production_table}" + suffix
                else:
                    # Fallback for safety, though pg_get_indexdef should be consistent
                    replicated_ddl = index_ddl.replace(production_table, new_production_table)
                cur.execute(replicated_ddl)

            # Perform the atomic swap
            logging.info("Performing atomic swap in a single transaction...")
            with self.conn.transaction():
                cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
                cur.execute(f"ALTER TABLE IF EXISTS {production_table} RENAME TO {backup_table};")
                cur.execute(f"ALTER TABLE {new_production_table} RENAME TO {production_table};")

        logging.info(f"Atomic swap complete for {production_table}. Production data is updated.")

    def _apply_delta_load(self, production_table: str, pk_name: str) -> None:
        """Applies inserts and soft deletes for a delta load."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Applying DELTA load for table {production_table}...")
        with self.conn.cursor() as cur:
            # Dynamically get the column list from the temp table for the insert
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'cdc_inserts' ORDER BY ordinal_position;"
            )
            columns = [row[0] for row in cur.fetchall()]
            column_list_str = ", ".join(columns)

            with self.conn.transaction():
                # Apply soft deletes
                sql_delete = f"""
                    UPDATE {production_table}
                    SET is_active = false, last_updated_at = NOW()
                    WHERE {pk_name} IN (SELECT id FROM cdc_deletes);
                """
                cur.execute(sql_delete)
                logging.info(f"Applied {cur.rowcount} soft deletes.")

                # Apply inserts
                sql_insert = f"""
                    INSERT INTO {production_table} ({column_list_str})
                    SELECT {column_list_str} FROM cdc_inserts;
                """
                cur.execute(sql_insert)
                logging.info(f"Applied {cur.rowcount} inserts.")

        logging.info(f"Delta load for {production_table} complete.")

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
