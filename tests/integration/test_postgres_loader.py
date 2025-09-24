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
    INITIAL_NAMES_DATA,
    DELTA_NAMES_DATA,
    STAGING_NAMES_DDL,
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
    generate_tsv_stream,
)

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


def _table_exists(cursor: psycopg.Cursor, table_name: str) -> bool:
    """Helper function to check if a table exists."""
    cursor.execute("SELECT to_regclass(%s)", (table_name,))
    return cursor.fetchone()[0] is not None


def _get_all_records(cursor: psycopg.Cursor, table_name: str) -> list[tuple]:
    """Helper function to get all records from a table."""
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY cui;")
    return cursor.fetchall()


def _get_active_records(cursor: psycopg.Cursor, table_name: str) -> list[tuple]:
    """Helper function to get active records from a table."""
    cursor.execute(f"SELECT * FROM {table_name} WHERE is_active = true ORDER BY cui;")
    return cursor.fetchall()


def test_full_load_atomic_swap(loader: PostgresNativeLoader, db_connection: psycopg.Connection):
    """
    Tests the full load process, validating the atomic swap.
    FRD Alignment: R-3.1.1, R-5.2.1, R-5.2.5
    """
    staging_table = "staging_names_full"
    prod_table = "medgen_names"
    prod_table_old = f"{prod_table}_old"

    # 1. Initialize and load staging table
    loader.initialize_staging(staging_table, STAGING_NAMES_DDL.format(table_name=staging_table))
    count = loader.bulk_load(staging_table, generate_tsv_stream(INITIAL_NAMES_DATA))
    db_connection.commit()
    assert count == len(INITIAL_NAMES_DATA)

    # 2. Apply changes
    metrics = loader.apply_changes(
        mode="full",
        staging_table=staging_table,
        production_table=prod_table,
        production_ddl=PRODUCTION_NAMES_DDL,
        index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
        pk_name="name_id",
    )
    db_connection.commit()

    # 3. Assertions
    assert metrics["inserted"] == len(INITIAL_NAMES_DATA)
    with db_connection.cursor() as cur:
        assert _table_exists(cur, prod_table)
        assert not _table_exists(cur, prod_table_old)

        active_records = _get_active_records(cur, prod_table)
        assert len(active_records) == len(INITIAL_NAMES_DATA)

        # Check for a specific record
        cur.execute("SELECT name FROM medgen_names WHERE cui = 'C0000001';")
        assert cur.fetchone()[0] == "Name A"


def test_full_load_idempotency(loader: PostgresNativeLoader, db_connection: psycopg.Connection):
    """
    Tests that running a full load twice results in a correct, non-duplicated state.
    FRD Alignment: R-3.3.1
    """
    staging_table = "staging_names_idem"
    prod_table = "medgen_names"

    # --- First Load ---
    loader.initialize_staging(staging_table, STAGING_NAMES_DDL.format(table_name=staging_table))
    loader.bulk_load(staging_table, generate_tsv_stream(INITIAL_NAMES_DATA))
    db_connection.commit()
    loader.apply_changes(
        mode="full",
        staging_table=staging_table,
        production_table=prod_table,
        production_ddl=PRODUCTION_NAMES_DDL,
        index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
        pk_name="name_id",
    )
    db_connection.commit()

    # --- Second Load (Identical) ---
    loader.initialize_staging(staging_table, STAGING_NAMES_DDL.format(table_name=staging_table))
    loader.bulk_load(staging_table, generate_tsv_stream(INITIAL_NAMES_DATA))
    db_connection.commit()
    metrics = loader.apply_changes(
        mode="full",
        staging_table=staging_table,
        production_table=prod_table,
        production_ddl=PRODUCTION_NAMES_DDL,
        index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
        pk_name="name_id",
    )
    db_connection.commit()

    # Assert final state is correct
    assert metrics["inserted"] == len(INITIAL_NAMES_DATA)
    with db_connection.cursor() as cur:
        active_records = _get_active_records(cur, prod_table)
        assert len(active_records) == len(INITIAL_NAMES_DATA)


def test_delta_load_end_to_end(loader: PostgresNativeLoader, db_connection: psycopg.Connection):
    """
    Tests the entire delta load process: inserts, updates, and soft deletes.
    FRD Alignment: R-3.2 (CDC), R-3.2.5 (Deletes), R-3.3.2 (Atomicity)
    """
    staging_table = "staging_names_delta"
    prod_table = "medgen_names"

    # --- Stage 1: Initial Full Load ---
    loader.initialize_staging(staging_table, STAGING_NAMES_DDL.format(table_name=staging_table))
    loader.bulk_load(staging_table, generate_tsv_stream(INITIAL_NAMES_DATA))
    db_connection.commit()
    loader.apply_changes(
        mode="full",
        staging_table=staging_table,
        production_table=prod_table,
        production_ddl=PRODUCTION_NAMES_DDL,
        index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
        pk_name="name_id",
    )
    db_connection.commit()

    # --- Stage 2: Delta Load ---
    loader.initialize_staging(staging_table, STAGING_NAMES_DDL.format(table_name=staging_table))
    loader.bulk_load(staging_table, generate_tsv_stream(DELTA_NAMES_DATA))
    db_connection.commit()

    # Execute CDC and apply changes
    loader.execute_cdc(
        staging_table=staging_table,
        production_table=prod_table,
        pk_name="name_id",
        business_key="cui",
    )
    metrics = loader.apply_changes(
        mode="delta",
        staging_table=staging_table,
        production_table=prod_table,
        pk_name="name_id",
        business_key="cui",
        # These are needed if the table needs to be created from scratch
        production_ddl=PRODUCTION_NAMES_DDL,
        index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
    )
    db_connection.commit()

    # --- Assert Final State ---
    assert metrics["inserted"] == 1
    assert metrics["updated"] == 1
    assert metrics["deleted"] == 1

    with db_connection.cursor() as cur:
        # Query all records, including inactive ones
        cur.execute("SELECT cui, name, is_active FROM medgen_names ORDER BY cui;")
        all_records = {rec[0]: (rec[1], rec[2]) for rec in cur.fetchall()}

        # Total records in table should be 4 (1 unchanged, 1 updated, 1 deleted, 1 new)
        assert len(all_records) == 4

        # Assert Unchanged: C0000001 should be 'Name A' and active
        assert all_records["C0000001"] == ("Name A", True)

        # Assert Update: C0000002 should be 'Name B Updated' and active
        assert all_records["C0000002"] == ("Name B Updated", True)

        # Assert Soft Delete: C0000003 should be 'Name C' and inactive
        assert all_records["C0000003"] == ("Name C", False)

        # Assert Insert: C0000004 should be 'Name D' and active
        assert all_records["C0000004"] == ("Name D", True)
