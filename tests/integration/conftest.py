import pytest
import psycopg
from testcontainers.postgres import PostgresContainer

from py_load_medgen.loader.postgres import PostgresNativeLoader


@pytest.fixture(scope="session")
def postgres_container() -> PostgresContainer:
    """
    A session-scoped fixture that starts a PostgreSQL container.
    The container is started once per test session and torn down at the end.
    """
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture
def db_connection(postgres_container: PostgresContainer) -> psycopg.Connection:
    """
    A function-scoped fixture that provides a fresh database connection
    for each test function, ensuring test isolation.
    """
    dsn = postgres_container.get_connection_url()
    # psycopg (v3) doesn't understand the "+psycopg2" dialect in the DSN
    # returned by testcontainers, so we remove it.
    dsn = dsn.replace("+psycopg2", "")
    with psycopg.connect(dsn) as conn:
        yield conn
    # The connection is automatically closed by the 'with' statement.


@pytest.fixture
def loader(db_connection: psycopg.Connection) -> PostgresNativeLoader:
    """
    A function-scoped fixture that provides an instance of the PostgresNativeLoader
    for each test, using the isolated database connection.
    autocommit is set to False to allow for manual transaction control in tests.
    """
    # The loader will use the provided connection without managing its lifecycle.
    # The db_connection fixture is responsible for opening and closing it.
    return PostgresNativeLoader(connection=db_connection, autocommit=False)
