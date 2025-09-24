import uuid
from abc import ABC, abstractmethod
from typing import Iterator, Optional, Self


class AbstractNativeLoader(ABC):
    """
    Abstract Base Class for database-specific native bulk loaders.
    This class defines the standard interface that all native loaders must
    implement to support the ETL process.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establishes a connection to the target database."""
        raise NotImplementedError

    def __enter__(self) -> Self:
        """Context manager entry point, establishes connection."""
        self.connect()
        return self

    @abstractmethod
    def close(self) -> None:
        """Closes the database connection."""
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, closes connection."""
        self.close()

    @abstractmethod
    def initialize_staging(self, table_name: str, ddl: str) -> None:
        """Creates and prepares a single staging table for data loading."""
        raise NotImplementedError

    @abstractmethod
    def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
        """
        Executes a native, high-performance bulk load operation.
        Args:
            table_name: The name of the target staging table.
            data_iterator: An iterator yielding bytes (e.g., TSV lines).
        """
        raise NotImplementedError

    @abstractmethod
    def execute_cdc(
        self, staging_table: str, production_table: str, pk_name: str, business_key: str
    ) -> dict[str, int]:
        """
        Executes the Change Data Capture (CDC) logic to identify inserts,
        updates, and deletes by comparing staging and production data.
        """
        raise NotImplementedError

    @abstractmethod
    def apply_changes(
        self,
        mode: str,
        staging_table: str,
        production_table: str,
        production_ddl: str,
        index_ddls: list[str],
        pk_name: str,
        business_key: str,
    ) -> None:
        """
        Applies the identified changes (inserts, updates, deletes) to the
        production tables atomically. This could involve a table swap or
        merge operations.
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup(self, staging_table: str, production_table: str) -> None:
        """Performs cleanup operations, such as dropping staging tables."""
        raise NotImplementedError

    @abstractmethod
    def log_run_start(
        self,
        run_id: uuid.UUID,
        package_version: str,
        load_mode: str,
        source_files: dict,
    ) -> int:
        """
        Logs the start of an ETL run and returns a unique identifier for the
        run log.
        """
        raise NotImplementedError

    @abstractmethod
    def log_run_finish(
        self,
        log_id: int,
        status: str,
        records_extracted: int,
        records_loaded: int,
        error_message: Optional[str] = None,
    ) -> None:
        """Logs the completion or failure of an ETL run."""
        raise NotImplementedError
