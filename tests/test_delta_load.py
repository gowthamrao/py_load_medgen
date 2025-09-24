import logging

import psycopg
import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import (
    MedgenName,
    MrconsoRecord,
    stream_tsv,
)
from py_load_medgen.sql.ddl import (
    PRODUCTION_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_INDEXES_DDL,
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
    STAGING_CONCEPTS_DDL,
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
# A business key identifies a unique record. For names, this is the
# combination of fields.
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
            cur.execute(
                "DROP TABLE IF EXISTS cdc_deletes; "
                "DROP TABLE IF EXISTS cdc_inserts; "
                "DROP TABLE IF EXISTS cdc_updates;"
            )
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute(
                "DROP TABLE IF EXISTS cdc_deletes; "
                "DROP TABLE IF EXISTS cdc_inserts; "
                "DROP TABLE IF EXISTS cdc_updates;"
            )


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
        v1_byte_iterator = stream_tsv(iter(V1_DATA))
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
            cur.execute(
                f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = true"
            )
            assert (
                cur.fetchone()[0] == 3
            ), "Initial delta load should insert 3 active records"

        # --- 2. Second Delta Load (V2 data) ---
        logging.info("--- Running Second Delta Load (V2) ---")
        loader.initialize_staging(STAGING_TABLE, STAGING_NAMES_DDL)
        v2_byte_iterator = stream_tsv(iter(V2_DATA))
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
            cur.execute(
                f"SELECT cui, name, is_active, suppress FROM {PRODUCTION_TABLE} "
                "ORDER BY cui, suppress"
            )
            results = cur.fetchall()

            assert (
                len(results) == 5
            ), "Total records should be 5 (3 initial + 2 new - 2 old)"

            active_records = [r for r in results if r["is_active"]]
            inactive_records = [r for r in results if not r["is_active"]]

            assert (
                len(active_records) == 3
            ), "There should be 3 active records after delta"
            assert (
                len(inactive_records) == 2
            ), "There should be 2 inactive (soft-deleted) records"

            # Check the state of each record
            record_states = {
                (r["cui"], r["suppress"]): r["is_active"] for r in results
            }

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

# V1: Initial dataset for concepts
V1_CONCEPTS_DATA = [
    MrconsoRecord(
        "C001", "ENG", "P", "L001", "PF", "S001", "Y", "A001", None, "M001", "D001",
        "SRC", "PT", "CODE1", "Record One", "0", "N", None, "raw1",
    ),
    MrconsoRecord(
        "C002", "ENG", "P", "L002", "PF", "S002", "Y", "A002", None, "M002", "D002",
        "SRC", "PT", "CODE2", "Record Two", "0", "N", None, "raw2",
    ),
    MrconsoRecord(
        "C003", "ENG", "P", "L003", "PF", "S003", "Y", "A003", None, "M003", "D003",
        "SRC", "PT", "CODE3", "Record Three", "0", "N", None, "raw3",
    ),
]

# V2: Updated dataset for concepts
# - A001: record_str is updated.
# - A002: record is deleted.
# - A004: new record is inserted.
V2_CONCEPTS_DATA = [
    MrconsoRecord(
        "C001", "ENG", "P", "L001", "PF", "S001", "Y", "A001", None, "M001", "D001",
        "SRC", "PT", "CODE1", "Record One Updated", "0", "N", None, "raw1_updated",
    ),
    MrconsoRecord(
        "C003", "ENG", "P", "L003", "PF", "S003", "Y", "A003", None, "M003", "D003",
        "SRC", "PT", "CODE3", "Record Three", "0", "N", None, "raw3",
    ),
    MrconsoRecord(
        "C004", "ENG", "P", "L004", "PF", "S004", "Y", "A004", None, "M004", "D004",
        "SRC", "PT", "CODE4", "Record Four", "0", "N", None, "raw4",
    ),
]


@pytest.mark.integration
def test_delta_load_with_updates(postgres_db_dsn) -> None:
    """
    Tests that the delta load correctly handles inserts, updates, and deletes.
    """
    STAGING_CONCEPTS_TABLE = "staging_medgen_concepts"
    PRODUCTION_CONCEPTS_TABLE = "medgen_concepts"
    CONCEPTS_PK = "concept_id"
    CONCEPTS_BUSINESS_KEY = "aui"

    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        conn = loader.conn
        loader._initialize_metadata()

        # --- 1. Initial Load (V1) ---
        loader.initialize_staging(STAGING_CONCEPTS_TABLE, STAGING_CONCEPTS_DDL)
        v1_byte_iterator = stream_tsv(iter(V1_CONCEPTS_DATA))
        loader.bulk_load(STAGING_CONCEPTS_TABLE, v1_byte_iterator)
        conn.commit()

        loader.apply_changes(
            mode="full",
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name=CONCEPTS_PK,
        )
        conn.commit()

        # --- 2. Delta Load (V2) ---
        loader.initialize_staging(STAGING_CONCEPTS_TABLE, STAGING_CONCEPTS_DDL)
        v2_byte_iterator = stream_tsv(iter(V2_CONCEPTS_DATA))
        loader.bulk_load(STAGING_CONCEPTS_TABLE, v2_byte_iterator)
        conn.commit()

        cdc_metrics = loader.execute_cdc(
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )

        assert cdc_metrics["inserts"] == 1
        assert cdc_metrics["updates"] == 1
        assert cdc_metrics["deletes"] == 1

        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )
        conn.commit()

        # --- Verification ---
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"SELECT aui, record_str, is_active FROM "
                f"{PRODUCTION_CONCEPTS_TABLE} ORDER BY aui"
            )
            results = cur.fetchall()

            assert len(results) == 4

            record_states = {
                r["aui"]: (r["record_str"], r["is_active"]) for r in results
            }

            # A001 was updated
            assert record_states["A001"] == ("Record One Updated", True)
            # A002 was deleted
            assert record_states["A002"] == ("Record Two", False)
            # A003 was unchanged
            assert record_states["A003"] == ("Record Three", True)
            # A004 was inserted
            assert record_states["A004"] == ("Record Four", True)


# --- Test Data for Reactivation Scenario ---

# V1: Initial dataset
REACTIVATION_V1 = [
    MrconsoRecord(
        "C1", "ENG", "P", "L1", "PF", "S1", "Y", "A1", None, "M1", "D1",
        "SRC", "PT", "CODE1", "Record 1, Version 1", "0", "N", None, "raw1",
    ),
    MrconsoRecord(
        "C2", "ENG", "P", "L2", "PF", "S2", "Y", "A2", None, "M2", "D2",
        "SRC", "PT", "CODE2", "Record 2", "0", "N", None, "raw2",
    ),
]

# V2: Record A1 is removed
REACTIVATION_V2 = [
    MrconsoRecord(
        "C2", "ENG", "P", "L2", "PF", "S2", "Y", "A2", None, "M2", "D2",
        "SRC", "PT", "CODE2", "Record 2", "0", "N", None, "raw2",
    ),
]

# V3: Record A1 is re-introduced with an updated string
REACTIVATION_V3 = [
    MrconsoRecord(
        "C1", "ENG", "P", "L1", "PF", "S1", "Y", "A1", None, "M1", "D1",
        "SRC", "PT", "CODE1", "Record 1, Version 2", "0", "N", None, "raw1_v2",
    ),
    MrconsoRecord(
        "C2", "ENG", "P", "L2", "PF", "S2", "Y", "A2", None, "M2", "D2",
        "SRC", "PT", "CODE2", "Record 2", "0", "N", None, "raw2",
    ),
]


@pytest.mark.integration
def test_delta_load_reactivation_scenario(postgres_db_dsn) -> None:
    """
    Tests that a soft-deleted record is correctly "reactivated" if it reappears
    in a subsequent data load.
    """
    STAGING_CONCEPTS_TABLE = "staging_medgen_concepts"
    PRODUCTION_CONCEPTS_TABLE = "medgen_concepts"
    CONCEPTS_PK = "concept_id"
    CONCEPTS_BUSINESS_KEY = "aui"

    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        conn = loader.conn
        loader._initialize_metadata()

        # --- 1. Initial Load (V1) ---
        logging.info("--- Running Initial Full Load (V1) ---")
        loader.initialize_staging(STAGING_CONCEPTS_TABLE, STAGING_CONCEPTS_DDL)
        v1_byte_iterator = stream_tsv(iter(REACTIVATION_V1))
        loader.bulk_load(STAGING_CONCEPTS_TABLE, v1_byte_iterator)
        conn.commit()

        loader.apply_changes(
            mode="full",
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name=CONCEPTS_PK,
        )
        conn.commit()

        # --- Verification for V1 ---
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(f"SELECT aui FROM {PRODUCTION_CONCEPTS_TABLE} WHERE is_active = true")
            results = {r["aui"] for r in cur.fetchall()}
            assert results == {"A1", "A2"}

        # --- 2. Delta Load (V2) - Soft-delete A1 ---
        logging.info("--- Running Delta Load (V2) to soft-delete A1 ---")
        loader.initialize_staging(STAGING_CONCEPTS_TABLE, STAGING_CONCEPTS_DDL)
        v2_byte_iterator = stream_tsv(iter(REACTIVATION_V2))
        loader.bulk_load(STAGING_CONCEPTS_TABLE, v2_byte_iterator)
        conn.commit()

        loader.execute_cdc(
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )
        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )
        conn.commit()

        # --- Verification for V2 ---
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"SELECT aui, is_active FROM {PRODUCTION_CONCEPTS_TABLE} ORDER BY aui"
            )
            results = {r["aui"]: r["is_active"] for r in cur.fetchall()}
            assert results["A1"] is False, "A1 should be soft-deleted"
            assert results["A2"] is True, "A2 should remain active"

        # --- 3. Delta Load (V3) - Reactivate A1 ---
        logging.info("--- Running Delta Load (V3) to reactivate A1 ---")
        loader.initialize_staging(STAGING_CONCEPTS_TABLE, STAGING_CONCEPTS_DDL)
        v3_byte_iterator = stream_tsv(iter(REACTIVATION_V3))
        loader.bulk_load(STAGING_CONCEPTS_TABLE, v3_byte_iterator)
        conn.commit()

        cdc_metrics = loader.execute_cdc(
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )

        assert cdc_metrics["updates"] == 1, "Reactivation should be counted as an update"
        assert cdc_metrics["inserts"] == 0
        assert cdc_metrics["deletes"] == 0

        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_CONCEPTS_TABLE,
            production_table=PRODUCTION_CONCEPTS_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name=CONCEPTS_PK,
            business_key=CONCEPTS_BUSINESS_KEY,
        )
        conn.commit()

        # --- Verification for V3 ---
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"SELECT aui, record_str, is_active "
                f"FROM {PRODUCTION_CONCEPTS_TABLE} ORDER BY aui"
            )
            results = {r["aui"]: (r["record_str"], r["is_active"]) for r in cur.fetchall()}
            assert results["A1"] == ("Record 1, Version 2", True), "A1 should be reactivated and updated"
            assert results["A2"] == ("Record 2", True), "A2 should remain active"
