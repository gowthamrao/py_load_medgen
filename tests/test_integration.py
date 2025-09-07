import io
import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, records_to_tsv

# Sample MRCONSO.RRF data for testing.
# Includes a valid record, a record with optional fields, and a blank line.
SAMPLE_MRCONSO_DATA = """\
C0001175|ENG|P|L0001175|VO|S0010340|Y|A0019182||M0000245|D000163|MSH|PM|D000163|Acquired Immunodeficiency Syndromes|0|N||
C0001175|ENG|S|L0001842|PF|S0011877|N|A2878223|103840012|62479008||SNOMEDCT_US|PT|62479008|AIDS|9|N|2304|

C0001175|FRE|S|L0162173|PF|S0226654|Y|A27478989||M0000245|D000163|MSHFRE|ET|D000163|SIDA|3|N||
"""


@pytest.mark.integration
def test_full_etl_pipeline(postgresql):
    """
    Tests the full ETL pipeline: parsing, TSV conversion, and loading into PostgreSQL.

    Args:
        postgresql: The pytest-postgresql fixture providing a live test database.
    """
    # Arrange
    staging_table = "staging_medgen_concepts"

    # Act: Run the ETL process using the connection from the fixture
    loader = PostgresNativeLoader(connection=postgresql)
    with loader:
        # 1. Initialize the staging table
        loader.initialize_staging()

        # 2. Parse the sample data and convert it to a TSV stream
        file_stream = io.StringIO(SAMPLE_MRCONSO_DATA)
        records_iterator = parse_mrconso(file_stream)
        tsv_stream = records_to_tsv(records_iterator)

        # 3. Bulk load the data into the staging table
        loader.bulk_load(staging_table, tsv_stream)

    # Assert: Verify the data in the database
    with postgresql.cursor() as cur:
        # Check that the correct number of rows were inserted (ignoring the blank line)
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        count = cur.fetchone()[0]
        assert count == 3

        # Check the content of the first loaded record
        cur.execute(f"SELECT cui, lat, str, sab FROM {staging_table} WHERE aui = 'A0019182'")
        record = cur.fetchone()
        assert record is not None
        assert record[0] == "C0001175"
        assert record[1] == "ENG"
        assert record[2] == "Acquired Immunodeficiency Syndromes"
        assert record[3] == "MSH"

        # Check the content of the second record, including an optional field (scui)
        cur.execute(f"SELECT scui FROM {staging_table} WHERE aui = 'A2878223'")
        record = cur.fetchone()
        assert record is not None
        assert record[0] == "62479008"
