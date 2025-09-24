# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import io
from dataclasses import dataclass, fields
from typing import List

from py_load_medgen.parser import _parse_pipe_delimited


@dataclass(frozen=True)
class SimpleRecord:
    """A simple dataclass for testing the generic parser."""

    field1: str
    field2: str
    field3: str
    raw_record: str


# The schema should not include the 'raw_record' field, which is added internally
SIMPLE_SCHEMA = [f.name for f in fields(SimpleRecord) if f.name != "raw_record"]


def test_parse_pipe_delimited_with_trailing_delimiters():
    """
    Tests that the _parse_pipe_delimited function correctly handles rows
    with trailing empty pipe delimiters, which can occur in some source files.
    FRD Alignment: R-2.2.3 (Error Handling)
    """
    # 1. Arrange: Create a stream with various valid and malformed rows
    content_lines = [
        "A|B|C",  # Valid
        "D|E|F|||",  # Valid, with trailing empty delimiters
        "G|H",  # Invalid, too few columns
        "I|J|K|L",  # Invalid, too many columns with content
        "M|N|O|",  # Valid, with one trailing empty delimiter
    ]
    file_stream = io.StringIO("\n".join(content_lines))
    filename = "test_trailing_delimiters.txt"

    # 2. Act: Parse the stream, allowing for errors
    records: List[SimpleRecord] = list(
        _parse_pipe_delimited(file_stream, SIMPLE_SCHEMA, SimpleRecord, filename, max_errors=10)
    )

    # 3. Assert
    assert len(records) == 3

    # Assert that the valid records were parsed correctly
    assert records[0] == SimpleRecord(field1="A", field2="B", field3="C", raw_record="A|B|C")
    assert records[1] == SimpleRecord(field1="D", field2="E", field3="F", raw_record="D|E|F|||")
    assert records[2] == SimpleRecord(field1="M", field2="N", field3="O", raw_record="M|N|O|")

    # The other two rows should have been skipped due to parsing errors.
