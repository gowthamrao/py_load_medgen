from abc import ABC, abstractmethod
from typing import Any, IO


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

    @abstractmethod
    def initialize_staging(self) -> None:
        """Creates and prepares staging tables for data loading."""
        raise NotImplementedError

    @abstractmethod
    def bulk_load(self, table_name: str, data_stream: IO[Any]) -> None:
        """
        Executes a native, high-performance bulk load operation.

        Args:
            table_name: The name of the target staging table.
            data_stream: A file-like object (stream) containing the data
                         to be loaded.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_cdc(self) -> None:
        """
        Executes the Change Data Capture (CDC) logic to identify inserts,
        updates, and deletes by comparing staging and production data.
        """
        raise NotImplementedError

    @abstractmethod
    def apply_changes(self) -> None:
        """
        Applies the identified changes (inserts, updates, deletes) to the
        production tables atomically. This could involve a table swap or
        merge operations.
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup(self) -> None:
        """Performs cleanup operations, such as dropping staging tables."""
        raise NotImplementedError
