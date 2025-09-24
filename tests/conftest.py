# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
from typing import Iterator

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_db_dsn() -> Iterator[str]:
    """
    A pytest fixture that starts a PostgreSQL container for the test session,
    and yields the database connection DSN.
    """
    with PostgresContainer("postgres:13-alpine") as postgres:
        dsn = postgres.get_connection_url()
        # psycopg3 doesn't like the 'psycopg2' driver specifier
        yield dsn.replace("postgresql+psycopg2://", "postgresql://")
