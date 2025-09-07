from typing import Any, IO, Optional
import psycopg

import logging
from typing import Any, IO, Optional
import psycopg

from py_load_medgen.loader.base import AbstractNativeLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


STAGING_CONCEPTS_DDL = """
CREATE UNLOGGED TABLE IF NOT EXISTS staging_medgen_concepts (
    cui VARCHAR(10) NOT NULL,
    lat VARCHAR(3) NOT NULL,
    ts CHAR(1),
    lui VARCHAR(10) NOT NULL,
    stt VARCHAR(20),
    sui VARCHAR(10) NOT NULL,
    ispref CHAR(1),
    aui VARCHAR(10),
    saui VARCHAR(10),
    scui VARCHAR(50),
    sdui VARCHAR(50),
    sab VARCHAR(40) NOT NULL,
    tty VARCHAR(40) NOT NULL,
    code VARCHAR(50) NOT NULL,
    str TEXT NOT NULL,
    srl VARCHAR(10) NOT NULL,
    suppress CHAR(1) NOT NULL,
    cvf VARCHAR(50)
);
"""

STAGING_NAMES_DDL = """
CREATE UNLOGGED TABLE IF NOT EXISTS staging_medgen_names (
    cui VARCHAR(10) NOT NULL,
    name TEXT NOT NULL,
    source VARCHAR(40) NOT NULL,
    suppress CHAR(1) NOT NULL
);
"""


class PostgresNativeLoader(AbstractNativeLoader):
    """
    A native loader for PostgreSQL that uses the COPY protocol for high-performance
    data ingestion.
    """

    def __init__(self, db_dsn: Optional[str] = None, connection: Optional[psycopg.Connection] = None):
        """
        Initializes the PostgreSQL loader.
        Accepts either a DSN string to create a new connection or an existing
        psycopg.Connection object (useful for testing).
        Args:
            db_dsn: The database connection string (DSN).
            connection: An existing psycopg.Connection object.
        """
        if not db_dsn and not connection:
            raise ValueError("Either db_dsn or connection must be provided.")

        self.dsn = db_dsn
        self.conn = connection
        self._managed_connection = connection is None

    def connect(self) -> None:
        """Establishes a connection to the PostgreSQL database if not already connected."""
        if self.conn and not self.conn.closed:
            logging.debug("Connection already established.")
            return

        if self._managed_connection and self.dsn:
            try:
                logging.info(f"Connecting to PostgreSQL database...")
                self.conn = psycopg.connect(self.dsn)
                logging.info("Connection successful.")
            except psycopg.Error as e:
                logging.error(f"Database connection error: {e}")
                raise
        elif not self._managed_connection:
            logging.debug("Using pre-existing connection. Connect logic skipped.")
        else:
            raise ConnectionError("Cannot connect without a DSN.")

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

    def bulk_load(self, table_name: str, data_stream: IO[Any]) -> None:
        """
        Executes a native, high-performance bulk load operation using COPY.
        Args:
            table_name: The name of the target staging table.
            data_stream: A file-like object (e.g., StringIO) containing
                         TSV-formatted data.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        logging.info(f"Starting bulk load into '{table_name}'...")
        with self.conn.cursor() as cur:
            with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT TEXT, NULL '\\N')") as copy:
                while chunk := data_stream.read(8192):
                    copy.write(chunk)
        logging.info(f"Bulk load into '{table_name}' complete.")

    def _create_and_load_new_production_table(
        self, cur: psycopg.Cursor, new_production_table: str, staging_table: str, index_ddls: list[str]
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
