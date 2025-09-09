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
