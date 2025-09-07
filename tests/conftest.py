import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres_db_dsn():
    """
    A pytest fixture that starts a PostgreSQL container for the test session,
    and yields the database connection DSN.
    """
    with PostgresContainer("postgres:13-alpine") as postgres:
        yield postgres.get_connection_url()
