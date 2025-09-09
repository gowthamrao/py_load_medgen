from unittest.mock import MagicMock

import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader

# A sample DDL for a production table for testing purposes
SAMPLE_PROD_DDL = """
CREATE TABLE {table_name} (
    cui CHAR(8) PRIMARY KEY,
    name TEXT
);
"""

@pytest.mark.unit
def test_apply_full_load_replicates_indexes(mocker):
    """
    Tests that the _apply_full_load method discovers indexes on the old
    production table and correctly replicates them on the new one.
    """
    # 1. Arrange
    # Mock the connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mocker.patch("psycopg.connect", return_value=mock_conn)

    # Spy on the cursor's execute method to track all SQL calls
    execute_spy = mocker.spy(mock_cursor, "execute")

    # Configure the mock cursor's fetchall to return different values
    # for different queries.
    mock_cursor.fetchall.side_effect = [
        # First call: _get_table_indexes for 'prod_table'
        [("CREATE INDEX prod_table_name_idx ON prod_table USING btree (name);",)],
        # Second call: get columns for the INSERT statement
        [("cui",), ("name",)],
    ]

    # Instantiate the loader with the mocked connection
    loader = PostgresNativeLoader(connection=mock_conn, autocommit=False)

    # 2. Act
    loader._apply_full_load(
        staging_table="staging_table",
        production_table="prod_table",
        production_ddl=SAMPLE_PROD_DDL,
    )

    # 3. Assert
    # This SQL must exactly match the string in the implementation.
    discovery_sql = (
        "SELECT indexdef FROM pg_indexes i JOIN pg_class c ON i.indexname = c.relname "
        "LEFT JOIN pg_constraint con ON c.oid = con.conindid "
        "WHERE i.tablename = %s AND con.contype IS DISTINCT FROM 'p';"
    )

    # Verify that the index discovery query was made on the original production table
    execute_spy.assert_any_call(discovery_sql, ("prod_table",))

    # Verify that the discovered index was replicated on the NEW production table,
    # with its original name intact.
    replicated_index_sql = (
        "CREATE INDEX prod_table_name_idx ON prod_table_new USING btree (name);"
    )
    execute_spy.assert_any_call(replicated_index_sql)

    # Also verify the atomic swap commands were called
    execute_spy.assert_any_call(
        "ALTER TABLE IF EXISTS prod_table RENAME TO prod_table_old;"
    )
    execute_spy.assert_any_call("ALTER TABLE prod_table_new RENAME TO prod_table;")
