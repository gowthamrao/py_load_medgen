import logging
import uuid
from datetime import datetime, timezone
from typing import Iterator, Optional

import psycopg

from py_load_medgen.loader.base import AbstractNativeLoader
from py_load_medgen.sql.ddl import (
    ETL_AUDIT_LOG_DDL,
    ETL_RUN_DETAILS_DDL,
    ETL_RUN_DETAILS_INDEX_DDL,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
        """Commits the transaction if a connection exists and autocommit is enabled."""
        if self.conn and not self.conn.closed and self.autocommit:
            self.conn.commit()

    def connect(self) -> None:
        """Establishes a connection and ensures metadata tables exist."""
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
                    "Managed connection was closed and cannot be "
                    "reopened without a DSN."
                )
            elif not self._managed_connection:
                raise ConnectionError("The provided external connection is closed.")
            else:
                raise ConnectionError("Cannot connect without a DSN.")
        self._initialize_metadata()

    def close(self) -> None:
        """
        Closes the database connection if it was created and is managed by this
        loader.
        """
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
        """Creates and prepares a single staging table for data loading."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info(f"Initializing staging table: {table_name}")
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            cur.execute(ddl)
        self._commit()
        logging.info(f"Staging table {table_name} initialized successfully.")

    def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> int:
        """
        Executes a native, high-performance bulk load operation using COPY.
        Returns:
            The number of rows loaded into the staging table.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info(f"Starting bulk load into '{table_name}'...")
        rowcount = 0
        with self.conn.cursor() as cur:
            with cur.copy(
                f"COPY {table_name} FROM STDIN WITH (FORMAT TEXT, NULL '\\N')"
            ) as copy:
                for line in data_iterator:
                    copy.write(line)
            rowcount = cur.rowcount
        self._commit()
        logging.info(f"Bulk load into '{table_name}' complete. Loaded {rowcount} rows.")
        return rowcount

    def _initialize_metadata(self) -> None:
        """Ensures the ETL audit log tables exist."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info("Initializing metadata tables: etl_audit_log, etl_run_details")
        with self.conn.cursor() as cur:
            cur.execute(ETL_AUDIT_LOG_DDL)
            cur.execute(ETL_RUN_DETAILS_DDL)
            cur.execute(ETL_RUN_DETAILS_INDEX_DDL)
        self._commit()
        logging.info("Metadata tables initialized.")

    def log_run_start(
        self,
        run_id: uuid.UUID,
        package_version: str,
        load_mode: str,
        source_files: dict,
        medgen_release_version: Optional[str] = None,
    ) -> int:
        """Logs the start of an ETL run and returns the log_id."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        sql = (
            "INSERT INTO etl_audit_log (run_id, package_version, load_mode, "
            "source_files, medgen_release_version, start_time, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING log_id;"
        )
        start_time = datetime.now(timezone.utc)
        source_files_json = psycopg.types.json.Jsonb(source_files)
        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    run_id,
                    package_version,
                    load_mode,
                    source_files_json,
                    medgen_release_version,
                    start_time,
                    "In Progress",
                ),
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
        sql = (
            "UPDATE etl_audit_log SET end_time = %s, status = %s, "
            "records_extracted = %s, records_loaded = %s, error_message = %s "
            "WHERE log_id = %s;"
        )
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

    def log_run_detail(self, log_id: int, metrics: dict) -> None:
        """Logs the detailed, per-table metrics of an ETL run."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        sql = """
            INSERT INTO etl_run_details (
                log_id, table_name, records_extracted, records_inserted,
                records_deleted, records_updated
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """

        with self.conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    log_id,
                    metrics.get("table_name"),
                    metrics.get("records_extracted", 0),
                    metrics.get("records_inserted", 0),
                    metrics.get("records_deleted", 0),
                    metrics.get("records_updated", 0),
                ),
            )
            self._commit()

        logging.info(f"Logged details for table: {metrics.get('table_name')}")

    def execute_cdc(
        self, staging_table: str, production_table: str, pk_name: str, business_key: str
    ) -> dict[str, int]:
        """Executes Change Data Capture (CDC) logic using SQL."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info(
            f"Executing CDC for {production_table} using key '{business_key}'..."
        )
        with self.conn.cursor() as cur:
            # Ensure temp tables exist and are empty for this run.
            cur.execute(
                "CREATE TEMP TABLE IF NOT EXISTS cdc_deletes (id BIGINT) "
                "ON COMMIT PRESERVE ROWS;"
            )
            cur.execute(
                f"CREATE TEMP TABLE IF NOT EXISTS cdc_inserts "
                f"(LIKE {staging_table} INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS;"
            )
            cur.execute(
                f"CREATE TEMP TABLE IF NOT EXISTS cdc_updates "
                f"(LIKE {staging_table} INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS;"
            )
            cur.execute("TRUNCATE TABLE cdc_deletes, cdc_inserts, cdc_updates;")

            cur.execute("SELECT to_regclass(%s)", (production_table,))
            table_exists = cur.fetchone()[0]

            delete_count = 0
            update_count = 0
            if table_exists:
                # Get the column names from the staging table to build a hash for
                # comparison. Exclude the raw_record column as it may contain
                # subtle differences that don't warrant an update.
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name != 'raw_record' "
                    "ORDER BY ordinal_position;",
                    (staging_table,),
                )
                columns_to_hash = [row[0] for row in cur.fetchall()]
                column_list_str = ", ".join([f's."{col}"' for col in columns_to_hash])

                # Construct the JOIN condition for one or more business keys
                keys = [key.strip() for key in business_key.split(",")]
                join_condition = " AND ".join([f"p.{key} = s.{key}" for key in keys])

                # --- Find Deletes ---
                where_condition_deletes = " AND ".join(
                    [f"s.{key} IS NULL" for key in keys]
                )
                sql_find_deletes = (
                    f"INSERT INTO cdc_deletes (id) SELECT p.{pk_name} "
                    f"FROM {production_table} p LEFT JOIN {staging_table} s "
                    f"ON {join_condition} WHERE {where_condition_deletes} "
                    "AND p.is_active = true;"
                )
                cur.execute(sql_find_deletes)
                delete_count = cur.rowcount

                # --- Find Updates ---
                # An update is a record that exists in both staging and prod, AND:
                # 1. The record is active and the content has changed.
                # OR
                # 2. The record was previously inactive (a "reactivation").
                hash_comparison = (
                    f"MD5(ROW({column_list_str})::TEXT) != "
                    f"MD5(ROW({column_list_str.replace('s.', 'p.')})::TEXT)"
                )
                sql_find_updates = (
                    f"INSERT INTO cdc_updates SELECT s.* FROM {staging_table} s "
                    f"JOIN {production_table} p ON {join_condition} "
                    f"WHERE (p.is_active = true AND {hash_comparison}) "
                    f"OR p.is_active = false;"
                )
                cur.execute(sql_find_updates)
                update_count = cur.rowcount

                # --- Find Inserts ---
                # Inserts are records in staging that are not in production
                # (by business key). We also need to exclude records that
                # were identified as updates.
                update_join_condition = " AND ".join(
                    [f"s.{key} = u.{key}" for key in keys]
                )
                sql_find_inserts = f"""
                    INSERT INTO cdc_inserts
                    SELECT s.*
                    FROM {staging_table} s
                    LEFT JOIN {production_table} p ON {join_condition}
                    LEFT JOIN cdc_updates u ON {update_join_condition}
                    WHERE p.{pk_name} IS NULL
                    AND u.{business_key.split(',')[0]} IS NULL;
                """
                cur.execute(sql_find_inserts)
                insert_count = cur.rowcount
            else:
                # If production table doesn't exist, all staging records are inserts
                logging.info(
                    f"Production table {production_table} does not exist. "
                    "Treating all records as inserts."
                )
                sql_find_inserts = (
                    f"INSERT INTO cdc_inserts SELECT s.* FROM {staging_table} s;"
                )
                cur.execute(sql_find_inserts)
                insert_count = cur.rowcount

        logging.info(
            f"CDC complete. Inserts: {insert_count}, "
            f"Updates: {update_count}, Deletes: {delete_count}"
        )
        return {
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count,
        }

    def _get_table_indexes(self, table_name: str) -> list[str]:
        """Retrieves the DDL for all non-primary-key indexes on a given table."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        logging.info(f"Discovering indexes for table: {table_name}")
        sql = (
            "SELECT indexdef FROM pg_indexes i "
            "JOIN pg_class c ON i.indexname = c.relname "
            "LEFT JOIN pg_constraint con ON c.oid = con.conindid "
            "WHERE i.tablename = %s AND con.contype IS DISTINCT FROM 'p';"
        )
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, (table_name,))
                index_ddls = [row[0] for row in cur.fetchall()]
                logging.info(
                    f"Found {len(index_ddls)} non-PK indexes for {table_name}."
                )
                return index_ddls
            except psycopg.errors.UndefinedTable:
                logging.warning(
                    f"Table '{table_name}' does not exist, cannot discover indexes. "
                    "Returning empty list."
                )
                return []

    def apply_changes(
        self,
        mode: str,
        staging_table: str,
        production_table: str,
        production_ddl: str,
        index_ddls: list[str],
        pk_name: str,
        business_key: Optional[str] = None,
        full_load_select_sql: Optional[str] = None,
    ) -> dict[str, int]:
        """
        Applies changes to the production table based on the load mode.
        Returns:
            A dictionary with the counts of inserted, updated, and deleted records.
        """
        if mode == "full":
            return self._apply_full_load(
                staging_table,
                production_table,
                production_ddl,
                full_load_select_sql,
            )
        elif mode == "delta":
            if not business_key:
                raise ValueError("A 'business_key' is required for delta loads.")
            return self._apply_delta_load(
                production_table, pk_name, business_key, production_ddl, index_ddls
            )
        else:
            raise ValueError(f"Unknown load mode: {mode}")

    def _apply_full_load(
        self,
        staging_table: str,
        production_table: str,
        production_ddl: str,
        full_load_select_sql: Optional[str] = None,
    ) -> dict[str, int]:
        """
        Applies changes atomically using the 'atomic swap' method for a full load.
        Returns:
            A dictionary with the count of inserted records.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        new_production_table = f"{production_table}_new"
        backup_table = f"{production_table}_old"
        index_ddls = self._get_table_indexes(production_table)
        inserted_count = 0

        logging.info(
            f"Applying FULL load for table {production_table} with atomic swap..."
        )
        with self.conn.cursor() as cur:
            cur.execute(production_ddl.format(table_name=new_production_table))

            if full_load_select_sql:
                insert_sql = full_load_select_sql.format(
                    new_production_table=new_production_table,
                    staging_table=staging_table,
                )
                logging.info(
                    f"Loading data into '{new_production_table}' using custom SQL..."
                )
                cur.execute(insert_sql)
                inserted_count = cur.rowcount
            else:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position;",
                    (staging_table,),
                )
                columns = [row[0] for row in cur.fetchall()]
                column_list_str = ", ".join(f'"{col}"' for col in columns)
                logging.info(
                    f"Loading data from '{staging_table}' into '{new_production_table}'"
                )
                cur.execute(
                    f"INSERT INTO {new_production_table} ({column_list_str}) "
                    f"SELECT {column_list_str} FROM {staging_table};"
                )
                inserted_count = cur.rowcount

            logging.info(
                f"Replicating {len(index_ddls)} indexes on "
                f"new table {new_production_table}..."
            )
            for index_ddl in index_ddls:
                prefix, sep, suffix = index_ddl.rpartition(f" ON {production_table}")
                replicated_ddl = (
                    (prefix + f" ON {new_production_table}" + suffix)
                    if sep
                    else index_ddl.replace(production_table, new_production_table)
                )
                cur.execute(replicated_ddl)

            logging.info("Performing atomic swap in a single transaction...")
            with self.conn.transaction():
                cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
                cur.execute(
                    f"ALTER TABLE IF EXISTS {production_table} "
                    f"RENAME TO {backup_table};"
                )
                cur.execute(
                    f"ALTER TABLE {new_production_table} RENAME TO {production_table};"
                )

        logging.info(
            f"Atomic swap complete for {production_table}. "
            f"Inserted {inserted_count} records."
        )
        return {"inserted": inserted_count, "updated": 0, "deleted": 0}

    def _apply_delta_load(
        self,
        production_table: str,
        pk_name: str,
        business_key: str,
        production_ddl: str,
        index_ddls: list[str],
    ) -> dict[str, int]:
        """
        Applies inserts, updates, and soft deletes for a delta load.
        Returns:
            A dictionary with the counts of inserted, updated, and deleted records.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        metrics = {"inserted": 0, "updated": 0, "deleted": 0}
        logging.info(f"Applying DELTA load for table {production_table}...")

        with self.conn.cursor() as cur:
            # Ensure production table and indexes exist
            cur.execute("SELECT to_regclass(%s)", (production_table,))
            if cur.fetchone()[0] is None:
                logging.info(
                    f"Production table '{production_table}' does not exist. "
                    "Creating now..."
                )
                cur.execute(production_ddl.format(table_name=production_table))
                for index_ddl in index_ddls:
                    cur.execute(index_ddl.format(table_name=production_table))
                logging.info(f"Table '{production_table}' and its indexes created.")

            business_key_cols = {key.strip() for key in business_key.split(",")}
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'cdc_updates' AND column_name != %s "
                "ORDER BY ordinal_position;",
                (pk_name,),
            )
            update_columns = [
                row[0] for row in cur.fetchall() if row[0] not in business_key_cols
            ]
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'cdc_inserts' AND column_name != %s "
                "ORDER BY ordinal_position;",
                (pk_name,),
            )
            insert_columns = [row[0] for row in cur.fetchall()]
            insert_column_list_str = ", ".join(f'"{col}"' for col in insert_columns)

            with self.conn.transaction():
                # 1. Apply Updates
                if update_columns:
                    set_clause = ", ".join(
                        [f'"{col}" = s."{col}"' for col in update_columns]
                    )
                    # When updating a record, always mark it as active. This handles
                    # both normal updates and "reactivations" of soft-deleted records.
                    set_clause += ", last_updated_at = NOW(), is_active = true"
                    keys = [key.strip() for key in business_key.split(",")]
                    join_condition = " AND ".join(
                        [f'p."{key}" = s."{key}"' for key in keys]
                    )
                    sql_update = (
                        f"UPDATE {production_table} p SET {set_clause} "
                        f"FROM cdc_updates s WHERE {join_condition};"
                    )
                    cur.execute(sql_update)
                    metrics["updated"] = cur.rowcount
                    logging.info(f"Applied {metrics['updated']} updates.")

                # 2. Apply Deletes
                sql_delete = (
                    f"UPDATE {production_table} SET is_active = false, "
                    f"last_updated_at = NOW() WHERE {pk_name} "
                    f"IN (SELECT id FROM cdc_deletes);"
                )
                cur.execute(sql_delete)
                metrics["deleted"] = cur.rowcount
                logging.info(f"Applied {metrics['deleted']} soft deletes.")

                # 3. Apply Inserts
                if insert_columns:
                    sql_insert = (
                        f"INSERT INTO {production_table} ({insert_column_list_str}) "
                        f"SELECT {insert_column_list_str} FROM cdc_inserts;"
                    )
                    cur.execute(sql_insert)
                    metrics["inserted"] = cur.rowcount
                    logging.info(f"Applied {metrics['inserted']} inserts.")

        logging.info(f"Delta load for {production_table} complete.")
        return metrics

    def cleanup(self, staging_table: str, production_table: str) -> None:
        """Performs cleanup operations by dropping old backup and staging tables."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")
        backup_table = f"{production_table}_old"
        logging.info(f"Performing cleanup for {production_table}...")
        with self.conn.cursor() as cur:
            with self.conn.transaction():
                cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
                cur.execute(f"DROP TABLE IF EXISTS {staging_table} CASCADE;")
                cur.execute(
                    "DROP TABLE IF EXISTS cdc_deletes; "
                    "DROP TABLE IF EXISTS cdc_inserts; "
                    "DROP TABLE IF EXISTS cdc_updates;"
                )
        logging.info(
            f"Cleanup complete. Dropped tables: {backup_table}, {staging_table}, "
            "cdc_deletes, cdc_inserts, cdc_updates"
        )
