import io
import pytest
import uuid
from psycopg.errors import UndefinedTable

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, stream_mrconso_tsv
from py_load_medgen.sql.ddl import STAGING_CONCEPTS_DDL, ETL_AUDIT_LOG_DDL

# Sample MRCONSO.RRF data for testing.
SAMPLE_MRCONSO_DATA = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|Acquired Immunodeficiency Syndromes|0|N||
C0001175|ENG|S|L0001842|PF|S0011877|N|A2878223|103840012|62479008||SNOMEDCT_US|PT|62479008|AIDS|9|N|2304|
C0001175|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|D000163|SIDA|3|N||
"""

STAGING_TABLE = "test_staging_concepts"
PRODUCTION_TABLE = "test_medgen_concepts"
INDEX_DDLS = [
    "ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (aui);",
    "CREATE INDEX idx_{table_name}_cui ON {table_name} (cui);",
]


@pytest.fixture(autouse=True)
def setup_teardown_tables(postgresql):
    """A fixture to ensure tables are dropped before and after each test."""
    # Setup: Drop tables if they exist to ensure a clean state
    with postgresql.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS etl_audit_log CASCADE;")
    yield
    # Teardown: Drop tables after the test
    with postgresql.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS etl_audit_log CASCADE;")


@pytest.mark.integration
def test_staging_load(postgresql):
    """
    Tests loading data into a staging table using the streaming pipeline.
    """
    with PostgresNativeLoader(connection=postgresql, autocommit=False) as loader:
        # 1. Initialize
        loader.initialize_staging(STAGING_TABLE, STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE))

        # 2. Parse, Transform, and Load
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        byte_iterator = stream_mrconso_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)

        # 3. Assert
        with postgresql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
            assert cur.fetchone()[0] == 3


@pytest.mark.integration
def test_full_load_atomic_swap(postgresql):
    """
    Tests the full load process, including atomic swap and cleanup.
    """
    backup_table = f"{PRODUCTION_TABLE}_old"

    with PostgresNativeLoader(connection=postgresql, autocommit=False) as loader:
        # 1. Run the full pipeline
        loader.initialize_staging(STAGING_TABLE, STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE))
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        byte_iterator = stream_mrconso_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)
        loader.apply_changes(STAGING_TABLE, PRODUCTION_TABLE, INDEX_DDLS)
        loader.cleanup(STAGING_TABLE, PRODUCTION_TABLE)

    # 2. Assert final state
    with postgresql.cursor() as cur:
        # Check that data is in the production table
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE}")
        assert cur.fetchone()[0] == 3
        cur.execute(f"SELECT cui, str FROM {PRODUCTION_TABLE} WHERE aui = 'A0019182'")
        record = cur.fetchone()
        assert record[0] == "C0001175"
        assert record[1] == "Acquired Immunodeficiency Syndromes"

        # Check that the staging and backup tables are gone
        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, STAGING_TABLE)
        assert not table_exists(cur, backup_table)


@pytest.mark.integration
def test_metadata_logging(postgresql):
    """
    Tests that the ETL process correctly logs metadata to the audit table.
    """
    run_id = uuid.uuid4()
    log_id = None

    with PostgresNativeLoader(connection=postgresql, autocommit=False) as loader:
        # 1. Log start
        log_id = loader.log_run_start(
            run_id=run_id,
            package_version="0.1.0-test",
            load_mode="full",
            source_files={"MRCONSO.RRF": "d41d8cd98f00b204e9800998ecf8427e"},
        )
        assert log_id is not None

        # Check that the 'In Progress' record was written
        with postgresql.cursor() as cur:
            cur.execute("SELECT status, package_version FROM etl_audit_log WHERE log_id = %s", (log_id,))
            record = cur.fetchone()
            assert record[0] == "In Progress"
            assert record[1] == "0.1.0-test"

        # 2. Log finish
        loader.log_run_finish(
            log_id,
            status="Succeeded",
            records_extracted=100,
            records_loaded=95,
        )

        # 3. Assert final state
        with postgresql.cursor() as cur:
            cur.execute(
                "SELECT status, records_extracted, records_loaded, error_message FROM etl_audit_log WHERE log_id = %s",
                (log_id,),
            )
            record = cur.fetchone()
            assert record[0] == "Succeeded"
            assert record[1] == 100
            assert record[2] == 95
            assert record[3] is None
