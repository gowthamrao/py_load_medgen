import gzip
import io
from pathlib import Path

import pytest
from py_load_medgen.parser import parse_mrconso, MrconsoRecord, parse_names, MedgenName

def test_parse_mrconso_handles_whitespace():
    """
    Tests that the MRCONSO parser is resilient to leading/trailing whitespace
    in fields.
    """
    # Note the extra spaces around the pipe delimiters and within the fields
    mock_data = (
        " C0000005 | ENG |P|L0000005|PF|S0007492|Y|A28464245| | | |MTH|PN|"
        " A-2-beta-glycoprotein-I | alpha-2-glycoprotein I |0|N|256||\n"
    )
    mock_file = io.StringIO(mock_data)

    # Parse the mock data
    parser = parse_mrconso(mock_file, max_errors=10)
    records = list(parser)

    # Assert that one record was parsed
    assert len(records) == 1
    record = records[0]

    # Assert that the fields were correctly stripped of whitespace
    assert record.cui == "C0000005"
    assert record.lat == "ENG"
    assert record.ispref == "Y"
    assert record.code == "A-2-beta-glycoprotein-I"
    assert record.record_str == "alpha-2-glycoprotein I"
    assert record.sab == "MTH"
    # The raw_record should remain untouched, with original whitespace
    assert record.raw_record == (
        " C0000005 | ENG |P|L0000005|PF|S0007492|Y|A28464245| | | |MTH|PN|"
        " A-2-beta-glycoprotein-I | alpha-2-glycoprotein I |0|N|256||"
    )

def test_parse_names_resilient_to_column_reordering(tmp_path: Path):
    """
    Tests that the parse_names function is resilient to the reordering of
    columns in the header, as required by FR-2.2.2.
    """
    # 1. Arrange: Create a dummy gzipped NAMES.RRF file with shuffled columns
    file_path = tmp_path / "NAMES_SHUFFLED.RRF.gz"

    # Header with a different column order: name, SUPPRESS, CUI, source
    shuffled_header = "#name|SUPPRESS|CUI|source|"

    # Data rows matching the shuffled header order
    content_lines = [
        shuffled_header,
        "Acute abdomen|N|C0000727|GTR|",
        "Abdominal cramps|N|C0000729|GTR|",
        "Abdominal distention|Y|C0000731|GTR|",
    ]

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content_lines))

    # 2. Act: Parse the file
    records = list(parse_names(file_path, max_errors=0))

    # 3. Assert: Check that records were parsed correctly despite shuffling
    assert len(records) == 3

    record1 = records[0]
    assert isinstance(record1, MedgenName)
    assert record1.cui == "C0000727"
    assert record1.name == "Acute abdomen"
    assert record1.source == "GTR"
    assert record1.suppress == "N"
    assert record1.raw_record == "Acute abdomen|N|C0000727|GTR|"

    record3 = records[2]
    assert record3.cui == "C0000731"
    assert record3.name == "Abdominal distention"
    assert record3.suppress == "Y"
