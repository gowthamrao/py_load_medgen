import logging

import psycopg
import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import MedgenName, stream_names_tsv
from py_load_medgen.sql.ddl import (
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
    STAGING_NAMES_DDL,
)

# --- Test Data ---

# V1: Initial dataset with 3 records
V1_DATA = [
    MedgenName("C001", "Name One", "SRC", "N", "C001|Name One|SRC|N|"),
    MedgenName("C002", "Name Two", "SRC", "N", "C002|Name Two|SRC|N|"),
    MedgenName("C003", "Name Three", "SRC", "Y", "C003|Name Three|SRC|Y|"),
]

# V2: Updated dataset
# - C001 is unchanged.
# - C002 is removed (should be soft-deleted).
# - C003 has its 'suppress' flag changed (will be treated as a delete-and-insert).
# - C004 is new.
V2_DATA = [
    MedgenName("C001", "Name One", "SRC", "N", "C001|Name One|SRC|N|"),
    MedgenName("C003", "Name Three", "SRC", "N", "C003|Name Three|SRC|N|"),
    MedgenName("C004", "Name Four", "SRC", "N", "C004|Name Four|SRC|N|"),
]

STAGING_TABLE = "staging_medgen_names"
PRODUCTION_TABLE = "medgen_names"
PK_NAME = "name_id"
# A business key identifies a unique record. For names, this is the combination of fields.
BUSINESS_KEY = "cui, name, source, suppress"


@pytest.fixture(autouse=True)
def setup_teardown_tables(postgres_db_dsn) -> None:
    """Ensures tables are dropped before and after each test for isolation."""
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            # Ensure CDC temp tables are gone
            cur.execute("DROP TABLE IF EXISTS cdc_deletes; DROP TABLE IF EXISTS cdc_inserts;")
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute("DROP TABLE IF EXISTS cdc_deletes; DROP TABLE IF EXISTS cdc_inserts;")


@pytest.mark.integration
def test_delta_load_and_soft_delete_scenario(postgres_db_dsn) -> None:
    """
    Tests the entire delta load workflow, including table creation on the first run,
    and the correct application of inserts and soft-deletes on a subsequent run.
    """
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        conn = loader.conn

        # --- 1. Initial Delta Load (V1 data) ---
        # This tests the fix: the delta load should create the production table.
        logging.info("--- Running Initial Delta Load (V1) ---")
        loader.initialize_staging(STAGING_TABLE, STAGING_NAMES_DDL)
        v1_byte_iterator = stream_names_tsv(iter(V1_DATA))
        loader.bulk_load(STAGING_TABLE, v1_byte_iterator)
        conn.commit()

        loader.execute_cdc(
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            pk_name=PK_NAME,
            business_key=BUSINESS_KEY,
        )
        # No commit here, CDC temp tables must persist until apply_changes

        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_NAMES_DDL,
            index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
            pk_name=PK_NAME,
            business_key=BUSINESS_KEY,
        )
        conn.commit()

        # --- Verification for Initial Load ---
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = true")
            assert cur.fetchone()[0] == 3, "Initial delta load should insert 3 active records"

        # --- 2. Second Delta Load (V2 data) ---
        logging.info("--- Running Second Delta Load (V2) ---")
        loader.initialize_staging(STAGING_TABLE, STAGING_NAMES_DDL)
        v2_byte_iterator = stream_names_tsv(iter(V2_DATA))
        loader.bulk_load(STAGING_TABLE, v2_byte_iterator)
        conn.commit()

        loader.execute_cdc(
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            pk_name=PK_NAME,
            business_key=BUSINESS_KEY,
        )
        # No commit here

        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_NAMES_DDL,
            index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
            pk_name=PK_NAME,
            business_key=BUSINESS_KEY,
        )
        conn.commit()

        # --- Verification for Second Load ---
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f"SELECT cui, name, is_active, suppress FROM {PRODUCTION_TABLE} ORDER BY cui, suppress")
            results = cur.fetchall()

            assert len(results) == 5, "Total records should be 5 (3 initial + 2 new - 2 old)"

            active_records = [r for r in results if r['is_active']]
            inactive_records = [r for r in results if not r['is_active']]

            assert len(active_records) == 3, "There should be 3 active records after delta"
            assert len(inactive_records) == 2, "There should be 2 inactive (soft-deleted) records"

            # Check the state of each record
            record_states = { (r['cui'], r['suppress']): r['is_active'] for r in results }

            # C001 was untouched, should be active
            assert record_states[("C001", "N")] is True
            # C002 was deleted, should be inactive
            assert record_states[("C002", "N")] is False
            # Old C003 was updated, so it should be inactive
            assert record_states[("C003", "Y")] is False
            # New C003 was inserted, should be active
            assert record_states[("C003", "N")] is True
            # C004 was inserted, should be active
            assert record_states[("C004", "N")] is True

        loader.cleanup(STAGING_TABLE, PRODUCTION_TABLE)
        conn.commit()
