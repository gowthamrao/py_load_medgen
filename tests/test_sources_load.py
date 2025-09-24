# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
from pathlib import Path

import pytest

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrsat, stream_tsv
from py_load_medgen.sql.ddl import (
    PRODUCTION_MEDGEN_SOURCES_DDL,
    PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
    STAGING_MEDGEN_SOURCES_DDL,
)

# Sample MRSAT.RRF data for testing
SAMPLE_MRSAT_DATA = """\
C0000005|L0000005|S0007492|A29346616|AUI||A29346616||OMIM_MIM_ID|OMIM|617943|N||
C0000005|||A29346617|CUI||A29346617||HPO_ID|HPO|HP:0000505|N||
C1234567|L1234567|S1234567|A87654321|AUI||A87654321||SOME_ID|OTHERSOURCE|ABC-123|N||
"""

STAGING_TABLE = "staging_medgen_sources"
PRODUCTION_TABLE = "medgen_sources"


@pytest.mark.integration
def test_full_load_of_sources(postgres_db_dsn: str, tmp_path: Path):
    """
    Tests the full ETL pipeline for a MRSAT.RRF file into the medgen_sources table.
    - Creates a staging table.
    - Parses and bulk loads data from a sample MRSAT file.
    - Applies the changes to a production table (atomic swap).
    - Verifies the data in the production table.
    """
    # 1. Setup: Create a dummy MRSAT.RRF file
    mrsat_file = tmp_path / "MRSAT.RRF"
    mrsat_file.write_text(SAMPLE_MRSAT_DATA)

    with PostgresNativeLoader(db_dsn=postgres_db_dsn) as loader:
        # 2. Initialize Staging
        loader.initialize_staging(STAGING_TABLE, STAGING_MEDGEN_SOURCES_DDL)

        # 3. Parse, Transform, and Load into Staging
        with open(mrsat_file, "r", encoding="utf-8") as f:
            records_iterator = parse_mrsat(f, max_errors=10)
            byte_iterator = stream_tsv(records_iterator)
            loader.bulk_load(STAGING_TABLE, byte_iterator)

        # 4. Apply Changes to Production
        loader.apply_changes(
            mode="full",
            staging_table=STAGING_TABLE,
            production_table=PRODUCTION_TABLE,
            production_ddl=PRODUCTION_MEDGEN_SOURCES_DDL,
            index_ddls=PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
            pk_name="source_id",
            full_load_select_sql="INSERT INTO {new_production_table} "
            "(cui, source_abbreviation, attribute_name, attribute_value, raw_record) "
            "SELECT cui, sab, atn, atv, raw_record FROM {staging_table};",
        )

        # 5. Verification
        with loader.conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {PRODUCTION_TABLE} WHERE is_active = true;"
            )
            count = cur.fetchone()[0]
            assert count == 3

            cur.execute(
                f"SELECT cui, source_abbreviation, attribute_name, attribute_value "
                f"FROM {PRODUCTION_TABLE} ORDER BY cui, attribute_name;"
            )
            results = cur.fetchall()

            # Assertions for the loaded data
            assert results[0] == ("C0000005", "HPO", "HPO_ID", "HP:0000505")
            assert results[1] == ("C0000005", "OMIM", "OMIM_MIM_ID", "617943")
            assert results[2] == ("C1234567", "OTHERSOURCE", "SOME_ID", "ABC-123")
