import io
import pytest
from py_load_medgen.parser import parse_mrconso, MrconsoRecord

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
    parser = parse_mrconso(mock_file, max_errors=0)
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
        "C0000005 | ENG |P|L0000005|PF|S0007492|Y|A28464245| | | |MTH|PN|"
        " A-2-beta-glycoprotein-I | alpha-2-glycoprotein I |0|N|256||"
    )
