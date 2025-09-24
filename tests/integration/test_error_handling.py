# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import pytest
import psycopg

from py_load_medgen.loader.postgres import PostgresNativeLoader
from tests.integration.test_data import (
    STAGING_NAMES_DDL,
    generate_tsv_stream,
)

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


def _get_all_records(cursor: psycopg.Cursor, table_name: str) -> list[tuple]:
    """Helper function to get all records from a table."""
    cursor.execute(f"SELECT * FROM {table_name};")
    return cursor.fetchall()


def test_bulk_load_handles_db_constraint_violation(
    loader: PostgresNativeLoader, db_connection: psycopg.Connection
):
    """
    Tests that the bulk loader correctly handles a database constraint
    violation (e.g., NOT NULL) and rolls back the transaction.
    FRD Alignment: R-4.2.1 (Error Handling), R-4.3.2 (Rollback)
    """
    staging_table = "staging_names_constraint_violation"

    # Data with a NULL value for the non-nullable 'cui' column.
    # The '\\N' is the standard way to represent NULL in PostgreSQL's TEXT format.
    # Each row must have 5 columns to match STAGING_NAMES_DDL.
    BAD_NAMES_DATA = [
        ["C0000001", "Name A", "SRC", "N", "raw_record_1"],
        ["\\N", "Name B", "SRC", "N", "raw_record_2_bad"],
        ["C0000003", "Name C", "SRC", "N", "raw_record_3"],
    ]

    # 1. Initialize staging table
    loader.initialize_staging(
        staging_table, STAGING_NAMES_DDL.format(table_name=staging_table)
    )
    db_connection.commit()

    # 2. Attempt to bulk load the malformed data
    # The entire COPY command should fail and be rolled back by the DB.
    with pytest.raises(psycopg.errors.NotNullViolation) as excinfo:
        loader.bulk_load(staging_table, generate_tsv_stream(BAD_NAMES_DATA))

    # Check that the error message is for the correct constraint violation
    assert "violates not-null constraint" in str(excinfo.value)
    assert 'column "cui"' in str(excinfo.value)

    # 3. Verify that the transaction was rolled back and the table is empty
    # We must rollback the failed transaction state before running new queries.
    db_connection.rollback()
    with db_connection.cursor() as cur:
        records = _get_all_records(cur, staging_table)
        assert len(records) == 0


def test_bulk_load_handles_data_type_mismatch(
    loader: PostgresNativeLoader, db_connection: psycopg.Connection
):
    """
    Tests that the bulk loader correctly handles a data type mismatch
    (e.g., loading a string into an INT column) and rolls back the transaction.
    FRD Alignment: R-4.2.1 (Error Handling), R-4.3.2 (Rollback)
    """
    staging_table = "staging_concepts_type_mismatch"

    # DDL with an integer column `concept_id`
    STAGING_CONCEPTS_DDL = """
    CREATE TABLE {table_name} (
        concept_id INTEGER,
        cui TEXT NOT NULL,
        name TEXT
    );
    """

    # Data with a string value for the integer `concept_id` column.
    BAD_CONCEPTS_DATA = [
        ["101", "C0000001", "Name A"],
        ["not_an_integer", "C0000002", "Name B"],
        ["103", "C0000003", "Name C"],
    ]

    # 1. Initialize staging table
    loader.initialize_staging(
        staging_table, STAGING_CONCEPTS_DDL.format(table_name=staging_table)
    )
    db_connection.commit()

    # 2. Attempt to bulk load the malformed data
    with pytest.raises(psycopg.errors.InvalidTextRepresentation) as excinfo:
        loader.bulk_load(staging_table, generate_tsv_stream(BAD_CONCEPTS_DATA))

    # Check that the error message is for the correct data type violation
    assert "invalid input syntax for type integer" in str(excinfo.value)
    assert '"not_an_integer"' in str(excinfo.value)

    # 3. Verify that the transaction was rolled back and the table is empty
    db_connection.rollback()
    with db_connection.cursor() as cur:
        records = _get_all_records(cur, staging_table)
        assert len(records) == 0
