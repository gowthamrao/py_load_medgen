import pytest

from py_load_medgen.loader.factory import LoaderFactory
from py_load_medgen.loader.postgres import PostgresNativeLoader


@pytest.mark.unit
def test_loader_factory_returns_postgres_loader() -> None:
    """
    Tests that the LoaderFactory correctly returns a PostgresNativeLoader
    for a valid PostgreSQL DSN.
    """
    dsn = "postgresql://user:pass@host:5432/dbname"
    loader = LoaderFactory.create_loader(dsn)
    assert isinstance(loader, PostgresNativeLoader)
    assert loader.dsn == dsn


@pytest.mark.unit
def test_loader_factory_raises_error_for_unsupported_scheme() -> None:
    """
    Tests that the LoaderFactory raises a ValueError for an unsupported DSN scheme.
    """
    dsn = "mysql://user:pass@host:3306/dbname"
    with pytest.raises(ValueError, match="Unsupported database scheme: 'mysql'"):
        LoaderFactory.create_loader(dsn)


@pytest.mark.unit
def test_loader_factory_raises_error_for_malformed_dsn() -> None:
    """
    Tests that the LoaderFactory raises a ValueError for a malformed DSN.
    """
    dsn = "not-a-valid-dsn"
    with pytest.raises(ValueError, match="Could not parse database DSN"):
        LoaderFactory.create_loader(dsn)
