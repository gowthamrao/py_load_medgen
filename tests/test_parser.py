import gzip
from pathlib import Path

from py_load_medgen.parser import MedgenName, parse_names


def test_parse_names(tmp_path: Path):
    """
    Tests that the parse_names function correctly parses a gzipped,
    pipe-delimited NAMES.RRF file.
    """
    # 1. Arrange: Create a dummy gzipped NAMES.RRF file
    file_path = tmp_path / "NAMES.RRF.gz"
    content = [
        "#CUI|name|source|SUPPRESS|",
        "C0000727|Acute abdomen|GTR|N|",
        "C0000729|Abdominal cramps|GTR|N|",
        "C0000731|Abdominal distention|GTR|Y|",  # Suppressed
        "C0000734|Abdominal mass|GTR|N|",
        "C0000735||GTR|N|",  # Valid row with empty name
        "C0000736|Missing pipe", # Malformed row
    ]
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content))

    # 2. Act: Parse the file
    records = list(parse_names(file_path))

    # 3. Assert
    assert len(records) == 5
    assert records[0] == MedgenName(cui="C0000727", name="Acute abdomen", source="GTR", suppress="N")
    assert records[1] == MedgenName(cui="C0000729", name="Abdominal cramps", source="GTR", suppress="N")
    assert records[2] == MedgenName(cui="C0000731", name="Abdominal distention", source="GTR", suppress="Y")
    assert records[3] == MedgenName(cui="C0000734", name="Abdominal mass", source="GTR", suppress="N")
    assert records[4] == MedgenName(cui="C0000735", name="", source="GTR", suppress="N")
