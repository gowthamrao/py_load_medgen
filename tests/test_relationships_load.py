# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import io

import psycopg
import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import (
    parse_mrrel,
    stream_tsv,
)
from py_load_medgen.sql.ddl import (
    PRODUCTION_MEDGEN_RELATIONSHIPS_DDL,
    PRODUCTION_MEDGEN_RELATIONSHIPS_INDEXES_DDL,
    STAGING_MEDGEN_RELATIONSHIPS_DDL,
)

# Sample MRREL.RRF data for testing.
SAMPLE_MRREL_DATA = """\
C0001175|A2878223|AUI|RO|C0001175|A2878224|AUI|has_finding_site|R12345678||MSH|MSH|||N||
C0001175|A2878223|AUI|RB|C0002395|A2878225|AUI||R23456789||SNOMEDCT_US|SNOMEDCT_US|||N||
C0002395|A2878225|AUI|RN|C0001175|A2878223|AUI|inverse_of_RO|R34567890||MSH|MSH|||N||
"""

STAGING_TABLE = "test_staging_relationships"
PRODUCTION_TABLE = "test_medgen_relationships"


@pytest.fixture(autouse=True)
def setup_teardown_tables(postgres_db_dsn):
    """A fixture to ensure tables are dropped before and after each test."""
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_audit_log CASCADE;")
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_audit_log CASCADE;")


@pytest.mark.integration
def test_full_load_relationships(postgres_db_dsn):
    """
    Tests the full load ETL process for the relationships (MRREL.RRF) data.
    """
    # 1. Arrange
    backup_table = f"{PRODUCTION_TABLE}_old"

    # 2. Act
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        # Initialize staging
        loader.initialize_staging(
            STAGING_TABLE,
            STAGING_MEDGEN_RELATIONSHIPS_DDL.replace(
                "staging_medgen_relationships", STAGING_TABLE
            ),
        )

        # Parse and bulk load
        file_stream = io.StringIO(SAMPLE_MRREL_DATA)
        records_iterator = parse_mrrel(file_stream, max_errors=10)
        byte_iterator = stream_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)

        # Apply changes (atomic swap)
        loader.apply_changes(
            mode="full",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_MEDGEN_RELATIONSHIPS_DDL.replace(
                "medgen_relationships", PRODUCTION_TABLE
            ),
            index_ddls=[
                ddl.replace("medgen_relationships", PRODUCTION_TABLE)
                for ddl in PRODUCTION_MEDGEN_RELATIONSHIPS_INDEXES_DDL
            ],
            pk_name="relationship_id",
            business_key="rui",
            # business_key is not used in full load, but required by signature
        )

        # Clean up
        loader.cleanup(STAGING_TABLE, PRODUCTION_TABLE)
        loader.conn.commit()

    # 3. Assert
    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        # Check production table content
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE}")
        assert cur.fetchone()[0] == 3

        cur.execute(
            f"SELECT cui1, cui2, rel, rela, sab FROM {PRODUCTION_TABLE} "
            f"WHERE rui = 'R12345678'"
        )
        record = cur.fetchone()
        assert record[0] == "C0001175"
        assert record[1] == "C0001175"
        assert record[2] == "RO"
        assert record[3] == "has_finding_site"
        assert record[4] == "MSH"

        # Check that optional fields are NULL
        cur.execute(f"SELECT rela FROM {PRODUCTION_TABLE} WHERE rui = 'R23456789'")
        record = cur.fetchone()
        assert record[0] is None

        # Check that staging and backup tables are gone
        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, STAGING_TABLE)
        assert not table_exists(cur, backup_table)
