# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
from urllib.parse import urlparse

from py_load_medgen.loader.base import AbstractNativeLoader
from py_load_medgen.loader.postgres import PostgresNativeLoader


class LoaderFactory:
    """
    A factory for creating database-specific loader instances.
    This class is responsible for selecting the correct concrete implementation
    of AbstractNativeLoader based on the provided database connection string (DSN).
    """

    @staticmethod
    def create_loader(db_dsn: str) -> AbstractNativeLoader:
        """
        Instantiates the appropriate native loader based on the DSN scheme.
        Args:
            db_dsn: The database connection string.
        Returns:
            A concrete instance of AbstractNativeLoader.
        Raises:
            ValueError: If the DSN scheme is unsupported.
        """
        try:
            parsed_uri = urlparse(db_dsn)
            scheme = parsed_uri.scheme
        except Exception as e:
            raise ValueError(f"Could not parse database DSN: {e}") from e

        if not scheme:
            raise ValueError("Could not parse database DSN: scheme is missing.")

        if scheme in ("postgres", "postgresql"):
            return PostgresNativeLoader(db_dsn=db_dsn)
        # Placeholder for future implementations
        # elif scheme == "redshift":
        #     return RedshiftNativeLoader(db_dsn=db_dsn)
        else:
            raise ValueError(
                f"Unsupported database scheme: '{scheme}'. "
                "Supported schemes are: 'postgresql'."
            )
