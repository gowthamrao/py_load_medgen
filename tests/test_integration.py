import io
import pytest
from psycopg.errors import UndefinedTable

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, records_to_tsv

# Sample MRCONSO.RRF data for testing.
SAMPLE_MRCONSO_DATA = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|Acquired Immunodeficiency Syndromes|0|N||
C0001175|ENG|S|L0001842|PF|S0011877|N|A2878223|103840012|62479008||SNOMEDCT_US|PT|62479008|AIDS|9|N|2304|
C0001175|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|D000163|SIDA|3|N||
"""


@pytest.mark.integration
def test_staging_load(postgresql):
    """
    Tests loading data into the staging table.
    Args:
        postgresql: The pytest-postgresql fixture providing a live test database.
    """
    loader = PostgresNativeLoader(connection=postgresql)
    with loader:
        loader.initialize_staging()
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        tsv_stream = records_to_tsv(records_iterator)
        loader.bulk_load(loader.staging_table, tsv_stream)

        with postgresql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {loader.staging_table}")
            assert cur.fetchone()[0] == 3


@pytest.mark.integration
def test_full_load_atomic_swap(postgresql):
    """
    Tests the full load process, including atomic swap and cleanup.
    Args:
        postgresql: The pytest-postgresql fixture.
    """
    loader = PostgresNativeLoader(connection=postgresql)
    production_table = loader.production_table
    staging_table = loader.staging_table
    backup_table = f"{production_table}_old"

    with loader:
        # 1. Run the full pipeline
        loader.initialize_staging()
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        tsv_stream = records_to_tsv(records_iterator)
        loader.bulk_load(loader.staging_table, tsv_stream)
        loader.apply_changes()
        loader.cleanup()

    # 2. Assert final state
    with postgresql.cursor() as cur:
        # Check that data is in the production table
        cur.execute(f"SELECT COUNT(*) FROM {production_table}")
        assert cur.fetchone()[0] == 3

        cur.execute(f"SELECT cui, str FROM {production_table} WHERE aui = 'A0019182'")
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

        assert not table_exists(cur, staging_table)
        assert not table_exists(cur, backup_table)
