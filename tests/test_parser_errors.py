import gzip
import io
from pathlib import Path

import pytest

from py_load_medgen.parser import (
    parse_mrconso,
    parse_names,
    parse_mrrel,
    parse_mrsty,
    ParsingError,
)


def test_parse_names_exceeds_max_errors(tmp_path: Path):
    """
    Tests that parse_names raises a ValueError when the number of
    malformed rows exceeds the max_errors threshold.
    """
    # 1. Arrange: Create a file with 3 malformed rows
    file_path = tmp_path / "NAMES.RRF.gz"
    content_lines = [
        "#CUI|name|source|SUPPRESS|",
        "C001|Good row|GTR|N|",
        "C002|Bad row 1",
        "C003|Good row|GTR|N|",
        "C004|Bad row 2",
        "C005|Bad row 3",
        "C006|Good row|GTR|N|",
    ]
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content_lines))

    # 2. Act & Assert: Set max_errors to 2 and expect a ParsingError
    with pytest.raises(ParsingError, match="Exceeded maximum parsing errors"):
        # We need to consume the iterator to trigger the parsing
        list(parse_names(file_path, max_errors=2))


def test_parse_names_within_max_errors(tmp_path: Path):
    """
    Tests that parse_names completes successfully when the number of
    malformed rows is within the max_errors threshold.
    """
    # 1. Arrange: Create a file with 2 malformed rows
    file_path = tmp_path / "NAMES.RRF.gz"
    content_lines = [
        "#CUI|name|source|SUPPRESS|",
        "C001|Good row|GTR|N|",
        "C002|Bad row 1",
        "C003|Good row|GTR|N|",
        "C004|Bad row 2",
        "C005|Good row|GTR|N|",
    ]
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content_lines))

    # 2. Act: Set max_errors to 2. This should not raise an exception.
    try:
        records = list(parse_names(file_path, max_errors=2))
    except ValueError:
        pytest.fail("parse_names raised ValueError unexpectedly.")

    # 3. Assert: Check that the correct number of good rows were parsed
    assert len(records) == 3


def test_parse_mrconso_exceeds_max_errors(tmp_path):
    """
    Tests that parse_mrconso raises a ValueError when the number of
    malformed rows exceeds the max_errors threshold.
    """
    # 1. Arrange: Create a file with a malformed row (too few columns)
    file_path = tmp_path / "MRCONSO.RRF"
    content_lines = [
        "C0000005|ENG|P|L0000005|PF|S0007492|Y|A26634265||M0019694|D012711|MSH|PEN|"
        "D012711|(131)I-Macroaggregated Albumin|0|N|256|",
        "C0000039|ENG|P|L0000039|PF|S0007563|Y|A26634304||M0023172|D015060|MSH|PEP|"
        "D015060|1,2-Dipalmitoylphosphatidylcholine|3",  # Truly malformed row
    ]
    file_path.write_text("\n".join(content_lines))


    # 2. Act & Assert: Set max_errors to 0 and expect a ParsingError
    with pytest.raises(ParsingError, match="Exceeded maximum parsing errors"):
        with open(file_path, "r") as f:
            list(parse_mrconso(f, max_errors=0))


def test_parse_mrrel_exceeds_max_errors():
    """
    Tests that parse_mrrel raises a ValueError when the error threshold is exceeded.
    """
    # 1. Arrange: A stream with one good row and one bad row
    content_lines = [
        "C0001175|A27478989|SCUI|RB|C0001290|A27478990|SCUI|SIDA|R222|S222|MSHFRE|MSHFRE|N|N|N|256|",
        "C0001175|A27478989|SCUI|RB|C0001290|A27478990|SCUI",  # Malformed
    ]
    file_stream = io.StringIO("\n".join(content_lines))

    # 2. Act & Assert: Set max_errors to 0
    with pytest.raises(ParsingError, match="Exceeded maximum parsing errors"):
        list(parse_mrrel(file_stream, max_errors=0))


def test_parse_mrsty_exceeds_max_errors():
    """
    Tests that parse_mrsty raises a ValueError when the error threshold is exceeded.
    """
    # 1. Arrange: A stream with one good row and one bad row
    content_lines = [
        "C0000074|T033|B1.2.1.1|Finding|AT12345|CVF123|",
        "C0000102|T061|B2.2.1.2.2",  # Malformed
    ]
    file_stream = io.StringIO("\n".join(content_lines))

    # 2. Act & Assert: Set max_errors to 0
    with pytest.raises(ParsingError, match="Exceeded maximum parsing errors"):
        list(parse_mrsty(file_stream, max_errors=0))
