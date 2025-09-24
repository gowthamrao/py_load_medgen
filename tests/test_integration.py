import gzip
import io
import uuid
from pathlib import Path

import psycopg
import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import (
    parse_hpo_mapping,
    parse_mrconso,
    parse_mrsty,
    stream_tsv,
)
from py_load_medgen.sql.ddl import (
    PRODUCTION_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_INDEXES_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
    PRODUCTION_SEMANTIC_TYPES_DDL,
    PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
    STAGING_CONCEPTS_DDL,
    STAGING_MEDGEN_HPO_MAPPING_DDL,
    STAGING_SEMANTIC_TYPES_DDL,
)

# Sample MRCONSO.RRF data for testing.
SAMPLE_MRCONSO_DATA = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|\
Acquired Immunodeficiency Syndromes|0|N||
C0001175|ENG|S|L0001842|PF|S0011877|N|A2878223|103840012|62479008||\
SNOMEDCT_US|PT|62479008|AIDS|9|N|2304|
C0001175|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|\
D000163|SIDA|3|N||
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
            cur.execute("DROP TABLE IF EXISTS etl_audit_log CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_run_details CASCADE;")
    yield
    with psycopg.connect(postgres_db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PRODUCTION_TABLE}_old CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_audit_log CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_run_details CASCADE;")


@pytest.mark.integration
def test_staging_load_with_raw_record(postgres_db_dsn):
    """
    Tests loading data into a staging table using testcontainers.
    """
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        loader.initialize_staging(
            STAGING_TABLE,
            STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE),
        )
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream, max_errors=10)
        byte_iterator = stream_tsv(records_iterator)
        loader.bulk_load(STAGING_TABLE, byte_iterator)
        loader.conn.commit()

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
        loader.initialize_staging(
            STAGING_TABLE,
            STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE),
        )
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream, max_errors=10)
        byte_iterator = stream_tsv(records_iterator)
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
        loader.conn.commit()

    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE}")
        assert cur.fetchone()[0] == 3
        cur.execute(
            f"SELECT cui, record_str, raw_record FROM {PRODUCTION_TABLE} "
            f"WHERE aui = 'A0019182'"
        )
        record = cur.fetchone()
        assert record[0] == "C0001175"
        assert record[1] == "Acquired Immunodeficiency Syndromes"
        assert record[2] == SAMPLE_MRCONSO_DATA.splitlines()[0]

        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = %s)",
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
    release_version = "Test Release 2025-09-07"

    # The 'with' statement ensures loader.connect() is called, which creates
    # metadata tables.
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=True) as loader:
        # 1. Test log_run_start
        log_id = loader.log_run_start(
            run_id=run_id,
            package_version="0.1.0-test",
            load_mode="full",
            source_files={"MRCONSO.RRF": "d41d8cd98f00b204e9800998ecf8427e"},
            medgen_release_version=release_version,
        )
        assert log_id is not None

        with loader.conn.cursor() as cur:
            cur.execute(
                "SELECT status, package_version, medgen_release_version "
                "FROM etl_audit_log WHERE log_id = %s",
                (log_id,),
            )
            record = cur.fetchone()
            assert record[0] == "In Progress"
            assert record[1] == "0.1.0-test"
            assert record[2] == release_version

        # 2. Test log_run_finish
        loader.log_run_finish(
            log_id,
            status="Succeeded",
            records_extracted=100,
            records_loaded=95,
        )

        with loader.conn.cursor() as cur:
            cur.execute(
                "SELECT status, records_extracted, records_loaded, error_message "
                "FROM etl_audit_log WHERE log_id = %s",
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
            STAGING_SEMANTIC_TYPES_DDL.replace(
                "staging_medgen_semantic_types", staging_table
            ),
        )

        # Parse and bulk load
        file_stream = io.StringIO(sample_mrsty_data)
        records_iterator = parse_mrsty(file_stream, max_errors=10)
        byte_iterator = stream_tsv(records_iterator)
        loader.bulk_load(staging_table, byte_iterator)

        # Apply changes (atomic swap)
        loader.apply_changes(
            mode="full",
            staging_table=staging_table,
            production_table=production_table,
            production_ddl=PRODUCTION_SEMANTIC_TYPES_DDL,
            index_ddls=PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
            pk_name="semantic_type_id",
            # pk_name is not used for full load, but required by signature
        )

        # Clean up
        loader.cleanup(staging_table, production_table)
        loader.conn.commit()

    # 3. Assert: Verify the final state of the database
    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        # Check production table content
        cur.execute(f"SELECT COUNT(*) FROM {production_table}")
        assert cur.fetchone()[0] == 3

        cur.execute(
            f"SELECT cui, sty, atui, cvf FROM {production_table} WHERE tui = 'T033'"
        )
        record = cur.fetchone()
        assert record[0] == "C0000074"
        assert record[1] == "Finding"
        assert record[2] == "AT12345"
        assert record[3] == "CVF123"

        # Check that staging and backup tables are gone
        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, staging_table)
        assert not table_exists(cur, backup_table)


@pytest.mark.integration
def test_delta_load_with_detailed_logging(postgres_db_dsn):
    """
    Tests the delta load process, including soft deletes, inserts,
    and the creation of detailed audit logs.
    """
    # 1. Arrange: Define initial and new data states
    # V1: 1 record to be kept, 1 to be deleted
    v1_data = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|\
Acquired Immunodeficiency Syndromes|0|N||
C9999999|ENG|P|L9999999|VO|S9999999|Y|A9999999||M9999999|D9999999|MSH|PM|D9999999|\
Record To Be Deleted|0|N||
"""
    # V2: The kept record, plus 1 new record
    v2_data = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|\
Acquired Immunodeficiency Syndromes|0|N||
C0001290|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|\
D000163|SIDA|3|N||
"""
    run_id = uuid.uuid4()

    # 2. Act: Setup initial state (V1) and run delta load (V2)
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        # Setup initial state (V1)
        loader.initialize_staging(
            STAGING_TABLE,
            STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE),
        )
        records_v1 = parse_mrconso(io.StringIO(v1_data), max_errors=10)
        loader.bulk_load(STAGING_TABLE, stream_tsv(records_v1))
        loader.apply_changes(
            mode="full",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name="concept_id"
        )
        loader.conn.commit()

        # Run the delta load process (V2)
        log_id = loader.log_run_start(run_id, "test", "delta", {})

        loader.initialize_staging(
            STAGING_TABLE,
            STAGING_CONCEPTS_DDL.replace("staging_medgen_concepts", STAGING_TABLE),
        )
        records_v2 = parse_mrconso(io.StringIO(v2_data), max_errors=10)
        loader.bulk_load(STAGING_TABLE, stream_tsv(records_v2))

        cdc_metrics = loader.execute_cdc(
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            pk_name="concept_id",
            business_key="aui"
        )
        loader.apply_changes(
            mode="delta",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_CONCEPTS_DDL,
            index_ddls=PRODUCTION_CONCEPTS_INDEXES_DDL,
            pk_name="concept_id",
            business_key="aui"
        )

        metrics = {
            "table_name": PRODUCTION_TABLE,
            "records_extracted": 2,
            "records_inserted": cdc_metrics.get("inserts", 0),
            "records_deleted": cdc_metrics.get("deletes", 0),
        }
        loader.log_run_detail(log_id, metrics)
        loader.conn.commit()

    # 4. Assert: Verify the final data state and audit logs
    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        # Assert data correctness
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = true")
        assert cur.fetchone()[0] == 2  # 1 kept, 1 new
        cur.execute(f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = false")
        assert cur.fetchone()[0] == 1  # 1 deleted
        cur.execute(
            f"SELECT cui FROM {PRODUCTION_TABLE} "
            "WHERE aui = 'A9999999' AND is_active = false"
        )
        assert (
            cur.fetchone()[0] == "C9999999"
        )  # Verify the correct record was soft-deleted
        cur.execute(
            f"SELECT cui FROM {PRODUCTION_TABLE} "
            "WHERE aui = 'A27478989' AND is_active = true"
        )
        assert cur.fetchone()[0] == "C0001290"  # Verify the new record was inserted

        # Assert detailed logging correctness
        cur.execute("SELECT log_id FROM etl_audit_log WHERE run_id = %s", (run_id,))
        log_id_from_db = cur.fetchone()[0]

        cur.execute(
            "SELECT records_inserted, records_deleted FROM etl_run_details "
            "WHERE log_id = %s AND table_name = %s",
            (log_id_from_db, PRODUCTION_TABLE),
        )
        detail_record = cur.fetchone()
        assert detail_record is not None
        assert detail_record[0] == 1  # 1 insert
        assert detail_record[1] == 1  # 1 delete


@pytest.mark.integration
def test_full_load_hpo_mapping(postgres_db_dsn, tmp_path: Path):
    """
    Tests the full load ETL process for the HPO Mapping data.
    """
    # 1. Arrange: Define constants and sample data
    staging_table = "test_staging_hpo_mapping"
    production_table = "test_medgen_hpo_mapping"
    backup_table = f"{production_table}_old"
    sample_hpo_data = (
        "C0000735\tHP:0001643\tShort QT interval\tShort QT interval\tHPO\tFinding\n"
        "C0001175\tHP:0000001\tAIDS\tAcquired immunodeficiency syndrome\tMSH\tDisease\n"
        "C0001290\tHP:0011849\tAbnormal bleeding\tBleeding\tHPO\tFinding\n"
    )

    # Helper to create gzipped test files
    def create_gzipped_file(data: str, filename: str) -> Path:
        file_path = tmp_path / filename
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            f.write(data)
        return file_path

    hpo_file = create_gzipped_file(sample_hpo_data, "hpo.txt.gz")

    # 2. Act: Run the ETL process
    with PostgresNativeLoader(db_dsn=postgres_db_dsn, autocommit=False) as loader:
        loader.initialize_staging(
            staging_table,
            STAGING_MEDGEN_HPO_MAPPING_DDL.replace(
                "staging_medgen_hpo_mapping", staging_table
            ),
        )

        records_iterator = parse_hpo_mapping(hpo_file, max_errors=10)
        byte_iterator = stream_tsv(records_iterator)
        loader.bulk_load(staging_table, byte_iterator)

        loader.apply_changes(
            mode="full",
            staging_table=staging_table,
            production_table=production_table,
            production_ddl=PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
            index_ddls=PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
            pk_name="hpo_mapping_id",
            business_key="sdui",
        )

        loader.cleanup(staging_table, production_table)
        loader.conn.commit()

    # 3. Assert: Verify the final state of the database
    with psycopg.connect(postgres_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {production_table}")
        assert cur.fetchone()[0] == 3

        cur.execute(
            f"SELECT cui, hpo_str, medgen_str, sty FROM {production_table} "
            f"WHERE sdui = 'HP:0000001'"
        )
        record = cur.fetchone()
        assert record[0] == "C0001175"
        assert record[1] == "AIDS"
        assert record[2] == "Acquired immunodeficiency syndrome"
        assert record[3] == "Disease"

        def table_exists(cursor, table_name):
            cursor.execute(
                "SELECT EXISTS (SELECT FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = %s)",
                (table_name,),
            )
            return cursor.fetchone()[0]

        assert not table_exists(cur, staging_table)
        assert not table_exists(cur, backup_table)
