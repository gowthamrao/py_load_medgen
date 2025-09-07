import io
import uuid
import psycopg
import pytest
from psycopg.errors import UndefinedTable

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, stream_mrconso_tsv, parse_mrsty, stream_mrsty_tsv
from py_load_medgen.sql.ddl import (
    STAGING_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_INDEXES_DDL,
    STAGING_SEMANTIC_TYPES_DDL,
    PRODUCTION_SEMANTIC_TYPES_DDL,
    PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
)

# Sample MRCONSO.RRF data for testing.
SAMPLE_MRCONSO_DATA = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|Acquired Immunodeficiency Syndromes|0|N||
C0001175|ENG|S|L0001842|PF|S0011877|N|A2878223|103840012|62479008||SNOMEDCT_US|PT|62479008|AIDS|9|N|2304|
C0001175|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|D000163|SIDA|3|N||
"""

STAGING_TABLE = "test_staging_concepts"
PRODUCTION_TABLE = "test_medgen_concepts"


@pytest.fixture(autouse=True)
def setup_teardown_tables(postgres_db_dsn):
    """A fixture to ensure tables are dropped before and after each test."""
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS etl_audit_log CASCADE;")
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS etl_audit_log CASCADE;")


@pytest.mark.integration
def test_staging_load_with_raw_record(postgres_db_dsn):
    """
    Tests loading data into a staging table using testcontainers.
    """
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        loader.initialize_staging(STAGING_TABLE, STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE))
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        byte_iterator = stream_mrconso_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)

        with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), MIN(raw_record) FROM {STAGING_TABLE}")
            count, raw_record = cur.fetchone()
            assert count == 3
            assert raw_record == SAMPLE_MRCONSO_DATA.splitlines()[0]


@pytest.mark.integration
def test_full_load_atomic_swap_with_raw_record(postgres_db_dsn):
    """
    Tests the full load process using testcontainers.
    """
    backup_table = f"{PRODUCTION_TABLE}_old"

    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        loader.initialize_staging(STAGING_TABLE, STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE))
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        byte_iterator = stream_mrconso_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)
        loader.apply_changes(
            mode="full",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name="concept_id"
        )
        loader.cleanup(STAGING_TABLE, PRODUCTION_TABLE)

    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE}")
        assert cur.fetchone()[0] == 3
        cur.execute(f"SELECT cui, str, raw_record FROM {PRODUCTION_TABLE} WHERE aui = 'A0019182'")
        record = cur.fetchone()
        assert record[0] == "C0001175"
        assert record[1] == "Acquired Immunodeficiency Syndromes"
        assert record[2] == SAMPLE_MRCONSO_DATA.splitlines()[0]

        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, STAGING_TABLE)
        assert not table_exists(cur, backup_table)


@pytest.mark.integration
def test_metadata_logging(postgres_db_dsn):
    """
    Tests metadata logging using a testcontainers database.
    """
    run_id = uuid.uuid4()
    log_id = None

    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        log_id = loader.log_run_start(
            run_id=run_id,
            package_version="0.1.0-test",
            load_mode="full",
            source_files={"MRCONSO.RRF": "d41d8cd98f00b204e9800998ecf8427e"},
        )
        assert log_id is not None

        with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT status, package_version FROM etl_audit_log WHERE log_id = %s", (log_id,))
            record = cur.fetchone()
            assert record[0] == "In Progress"
            assert record[1] == "0.1.0-test"

        loader.log_run_finish(
            log_id,
            status="Succeeded",
            records_extracted=100,
            records_loaded=95,
        )

        with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status, records_extracted, records_loaded, error_message FROM etl_audit_log WHERE log_id = %s",
                (log_id,),
            )
            record = cur.fetchone()
            assert record[0] == "Succeeded"
            assert record[1] == 100
            assert record[2] == 95
            assert record[3] is None


@pytest.mark.integration
def test_full_load_semantic_types(postgres_db_dsn):
    """
    Tests the full load ETL process for the semantic types (MRSTY.RRF) data.
    """
    # 1. Arrange: Define constants and sample data
    staging_table = "test_staging_semantic_types"
    production_table = "test_medgen_semantic_types"
    backup_table = f"{production_table}_old"
    sample_mrsty_data = """\
C0000052|T029|B1.2.1.2.1|Body Part, Organ, or Organ Component|||
C0000074|T033|B1.2.1.1|Finding|AT12345|CVF123|
C0000097|T121|B4|Pharmacologic Substance|||
"""

    # 2. Act: Run the ETL process
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        # Initialize staging
        loader.initialize_staging(
            staging_table,
            STAGING_SEMANTIC_TYPES_DDL.replace("staging_medgen_semantic_types", staging_table)
        )

        # Parse and bulk load
        file_stream = io.StringIO(sample_mrsty_data)
        records_iterator = parse_mrsty(file_stream)
        byte_iterator = stream_mrsty_tsv(records_iterator)
        loader.bulk_load(staging_table, byte_iterator)

        # Apply changes (atomic swap)
        loader.apply_changes(
            mode="full",
            staging_table=staging_table,
            production_table=production_table,
            production_ddl=PRODUCTION_SEMANTIC_TYPES_DDL,
            index_ddls=PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
            pk_name="semantic_type_id" # pk_name is not used for full load, but required by signature
        )

        # Clean up
        loader.cleanup(staging_table, production_table)

    # 3. Assert: Verify the final state of the database
    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        # Check production table content
        cur.execute(f"SELECT COUNT(*) FROM {production_table}")
        assert cur.fetchone()[0] == 3

        cur.execute(f"SELECT cui, sty, atui, cvf FROM {production_table} WHERE tui = 'T033'")
        record = cur.fetchone()
        assert record[0] == "C0000074"
        assert record[1] == "Finding"
        assert record[2] == "AT12345"
        assert record[3] == "CVF123"

        # Check that staging and backup tables are gone
        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, staging_table)
        assert not table_exists(cur, backup_table)
