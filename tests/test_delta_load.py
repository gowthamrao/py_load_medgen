import gzip
import io
from pathlib import Path

import psycopg
import pytest
from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_names, stream_names_tsv
from py_load_medgen.sql.ddl import (
    STAGING_NAMES_DDL,
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
)

# --- Test Data ---
# Version 1: Initial dataset
V1_DATA = """\
C001|First Name|SRC1|N|C001|First Name|SRC1|N
C002|Second Name|SRC2|N|C002|Second Name|SRC2|N
C003|Third Name|SRC3|Y|C003|Third Name|SRC3|Y
"""

# Version 2: Updated dataset
# - C001 is unchanged.
# - C002 is removed (should be soft-deleted).
# - C003 has its suppress flag changed (should be soft-deleted and re-inserted).
# - C004 is new (should be inserted).
V2_DATA = """\
C001|First Name|SRC1|N|C001|First Name|SRC1|N
C003|Third Name|SRC3|N|C003|Third Name|SRC3|N
C004|Fourth Name|SRC4|N|C004|Fourth Name|SRC4|N
"""

STAGING_TABLE = "test_staging_names"
PRODUCTION_TABLE = "test_medgen_names"
PK_NAME = "name_id"


@pytest.fixture(autouse=True)
def setup_teardown_tables(postgres_db_dsn):
    """Ensures tables are dropped before and after each test for isolation."""
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")


@pytest.mark.integration
def test_delta_load_scenario(postgres_db_dsn, tmp_path: Path):
    """
    Tests the entire delta load workflow using testcontainers.
    """
    # Helper to create gzipped test files
    def create_gzipped_file(data: str, filename: str) -> Path:
        file_path = tmp_path / filename
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            # Add a header to be skipped by the parser
            f.write("#CUI|name|source|SUPPRESS|\n")
            f.write(data)
        return file_path

    v1_file = create_gzipped_file(V1_DATA, "v1.gz")
    v2_file = create_gzipped_file(V2_DATA, "v2.gz")

    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        # --- Phase 1: Initial Full Load ---
        # A. Initialize Staging and Production tables
        loader.initialize_staging(STAGING_TABLE, STAGING_NAMES_DDL.replace("staging_medgen_names", STAGING_TABLE))
        with psycopg.connect(postgres_db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(PRODUCTION_NAMES_DDL.format(table_name=PRODUCTION_TABLE))
                conn.commit()

        # B. Load V1 data and apply as a full load
        v1_records = parse_names(v1_file)
        v1_bytes = stream_names_tsv(v1_records)
        loader.bulk_load(STAGING_TABLE, v1_bytes)
        loader.apply_changes(
            mode="full",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_NAMES_DDL,
            index_ddls=PRODUCTION_NAMES_INDEXES_DDL,
            pk_name=PK_NAME,
        )

        # C. Verify initial state
        with psycopg.connect(postgres_db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = true")
                assert cur.fetchone()[0] == 3

        # --- Phase 2: Delta Load ---
        # A. Load V2 data into staging
        loader.initialize_staging(STAGING_TABLE, STAGING_NAMES_DDL.replace("staging_medgen_names", STAGING_TABLE))
        v2_records = parse_names(v2_file)
        v2_bytes = stream_names_tsv(v2_records)
        loader.bulk_load(STAGING_TABLE, v2_bytes)

        # B. Execute CDC and apply delta changes
        loader.execute_cdc(STAGING_TABLE, PRODUCTION_TABLE, PK_NAME)
        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl="",  # Not used in delta
            index_ddls=[],     # Not used in delta
            pk_name=PK_NAME,
        )

        # --- Phase 3: Verification ---
        with psycopg.connect(postgres_db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT cui, name, is_active, suppress FROM {PRODUCTION_TABLE} ORDER BY cui, name, suppress")
                results = cur.fetchall()

                assert len(results) == 5

                active_records = [r for r in results if r[2]]
                inactive_records = [r for r in results if not r[2]]

                assert len(active_records) == 3
                assert ("C001", "First Name", True, "N") in active_records
                assert ("C003", "Third Name", True, "N") in active_records
                assert ("C004", "Fourth Name", True, "N") in active_records

                assert len(inactive_records) == 2
                assert ("C002", "Second Name", False, "N") in inactive_records
                assert ("C003", "Third Name", False, "Y") in inactive_records
