# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import io

import pytest

from py_load_medgen.parser import MrstyRecord, parse_mrsty, ParsingError


def test_parse_mrsty_valid_record():
    """Tests that a single valid MRSTY.RRF record is parsed correctly."""
    raw_data = "C0001175|T047|B2.2.1.2.1|Disease or Syndrome|AT17683839|2304|\n"
    file_stream = io.StringIO(raw_data)
    records = list(parse_mrsty(file_stream, max_errors=10))

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, MrstyRecord)
    assert record.cui == "C0001175"
    assert record.tui == "T047"
    assert record.stn == "B2.2.1.2.1"
    assert record.sty == "Disease or Syndrome"
    assert record.atui == "AT17683839"
    assert record.cvf == "2304"
    assert record.raw_record == raw_data.strip()


def test_parse_mrsty_missing_optional_fields():
    """Tests parsing a record where optional fields (atui, cvf) are empty."""
    raw_data = "C0001175|T047|B2.2.1.2.1|Disease or Syndrome|||\n"
    file_stream = io.StringIO(raw_data)
    records = list(parse_mrsty(file_stream, max_errors=10))

    assert len(records) == 1
    record = records[0]
    assert record.atui is None
    assert record.cvf is None
    assert record.raw_record == raw_data.strip()


def test_parse_mrsty_malformed_line():
    """Tests that a malformed line with too few columns is skipped."""
    raw_data = "C0001175|T047|B2.2.1.2.1\n"  # Missing columns
    file_stream = io.StringIO(raw_data)
    records = list(parse_mrsty(file_stream, max_errors=10))

    assert len(records) == 0


def test_parse_mrsty_empty_stream():
    """Tests that parsing an empty file results in no records."""
    file_stream = io.StringIO("")
    records = list(parse_mrsty(file_stream, max_errors=10))

    assert len(records) == 0


def test_parse_mrsty_exceeds_max_errors():
    """Tests that a ParsingError is raised if malformed lines exceed max_errors."""
    raw_data = "invalid1|\n" "invalid2|\n" "invalid3|\n"
    file_stream = io.StringIO(raw_data)

    with pytest.raises(ParsingError) as excinfo:
        list(parse_mrsty(file_stream, max_errors=2))

    assert "Exceeded maximum parsing errors (2)" in str(excinfo.value)
