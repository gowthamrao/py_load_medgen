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
    aui VARCHAR(10) PRIMARY KEY,
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
        self._managed_connection = connection is None  # Flag to manage connection lifecycle

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

    def initialize_staging(self) -> None:
        """Creates and prepares staging tables for data loading."""
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        table_name = "staging_medgen_concepts"
        logging.info(f"Initializing staging table: {table_name}")
        with self.conn.cursor() as cur:
            # Use a transaction for the DDL operations
            with self.conn.transaction():
                cur.execute(STAGING_CONCEPTS_DDL)
                cur.execute(f"TRUNCATE TABLE {table_name};")
        logging.info("Staging table initialized successfully.")

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
                # Read from the stream in chunks to manage memory
                while chunk := data_stream.read(8192):
                    copy.write(chunk)

        logging.info(f"Bulk load into '{table_name}' complete.")

    def execute_cdc(self) -> None:
        """Executes the Change Data Capture (CDC) logic (placeholder)."""
        logging.warning("`execute_cdc` is not yet implemented.")
        pass

    def apply_changes(self) -> None:
        """Applies the identified changes atomically (placeholder)."""
        logging.warning("`apply_changes` is not yet implemented.")
        pass

    def cleanup(self) -> None:
        """Performs cleanup operations (placeholder)."""
        logging.warning("`cleanup` is not yet implemented.")
        pass
