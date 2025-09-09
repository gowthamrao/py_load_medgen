import gzip
import io
from pathlib import Path

from py_load_medgen.parser import (
    MedgenHpoMapping,
    MedgenName,
    MrconsoRecord,
    MrstyRecord,
    parse_hpo_mapping,
    parse_mrconso,
    parse_mrsty,
    parse_names,
)


def test_parse_names_and_raw_record(tmp_path: Path):
    """
    Tests that the parse_names function correctly parses a gzipped,
    pipe-delimited NAMES.RRF file and captures the raw record.
    """
    # 1. Arrange: Create a dummy gzipped NAMES.RRF file
    file_path = tmp_path / "NAMES.RRF.gz"
    content_lines = [
        "#CUI|name|source|SUPPRESS|",
        "C0000727|Acute abdomen|GTR|N|",
        "C0000729|Abdominal cramps|GTR|N|",
        "C0000731|Abdominal distention|GTR|Y|",  # Suppressed
        "C0000734|Abdominal mass|GTR|N|",
        "C0000735||GTR|N|",  # Valid row with empty name
        "C0000736|Missing pipe",  # Malformed row
    ]
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content_lines))

    # 2. Act: Parse the file, allowing for some errors
    records = list(parse_names(file_path, max_errors=10))

    # 3. Assert
    assert len(records) == 5
    assert records[0] == MedgenName(
        cui="C0000727",
        name="Acute abdomen",
        source="GTR",
        suppress="N",
        raw_record=content_lines[1],
    )
    assert records[1] == MedgenName(
        cui="C0000729",
        name="Abdominal cramps",
        source="GTR",
        suppress="N",
        raw_record=content_lines[2],
    )
    assert records[2] == MedgenName(
        cui="C0000731",
        name="Abdominal distention",
        source="GTR",
        suppress="Y",
        raw_record=content_lines[3],
    )
    assert records[3] == MedgenName(
        cui="C0000734",
        name="Abdominal mass",
        source="GTR",
        suppress="N",
        raw_record=content_lines[4],
    )
    assert records[4] == MedgenName(
        cui="C0000735", name="", source="GTR", suppress="N", raw_record=content_lines[5]
    )


def test_parse_hpo_mapping(tmp_path: Path):
    """
    Tests that the parse_hpo_mapping function correctly parses a gzipped,
    tab-delimited MedGen_HPO_Mapping.txt.gz file.
    """
    # 1. Arrange: Create a dummy gzipped MedGen_HPO_Mapping.txt.gz file
    file_path = tmp_path / "MedGen_HPO_Mapping.txt.gz"
    # Based on FTP_README.txt, the columns are:
    # CUI, SDUI, HpoStr, MedGenStr, MedGenStr_SAB, STY
    content_lines = [
        "#CUI\tSDUI\tHpoStr\tMedGenStr\tMedGenStr_SAB\tSTY",
        "C0000768\tHP:0002164\tAbducens palsy\tAbducens palsy\tHPO\tFinding",
        "C0001261\tHP:0000522\tAlacrima\tAlacrima\tHPO\tFinding",
        "C0001261\tHP:0007784\tAbsent lacrimal punctum\tAbsent lacrimal punctum\tHPO\tFinding",
        "C0001439\tHP:0001290\tAphasia\tAphasia\tHPO\tFinding",
        "C0001439\tHP:00024Aphasia\tAphasia\tHPO\tFinding",  # Malformed row (missing tab)
    ]
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(content_lines))

    # 2. Act: Parse the file, allowing for some errors
    records = list(parse_hpo_mapping(file_path, max_errors=10))

    # 3. Assert
    assert len(records) == 4
    assert records[0] == MedgenHpoMapping(
        cui="C0000768",
        sdui="HP:0002164",
        hpo_str="Abducens palsy",
        medgen_str="Abducens palsy",
        medgen_str_sab="HPO",
        sty="Finding",
        raw_record=content_lines[1],
    )
    assert records[1].cui == "C0001261"
    assert records[1].sdui == "HP:0000522"
    assert records[3] == MedgenHpoMapping(
        cui="C0001439",
        sdui="HP:0001290",
        hpo_str="Aphasia",
        medgen_str="Aphasia",
        medgen_str_sab="HPO",
        sty="Finding",
        raw_record=content_lines[4],
    )


def test_parse_mrconso_and_raw_record():
    """
    Tests that the parse_mrconso function correctly parses a pipe-delimited
    MRCONSO.RRF file stream and captures the raw record.
    """
    # 1. Arrange: Create a dummy MRCONSO.RRF file stream
    content_lines = [
        "C0000005|ENG|P|L0000005|PF|S0007492|Y|A26634265||M0019694|D012711|MSH|PEN|"
        "D012711|(131)I-Macroaggregated Albumin|0|N|256|",
        "C0000039|ENG|P|L0000039|PF|S0007563|Y|A26634304||M0023172|D015060|MSH|PEP|"
        "D015060|1,2-Dipalmitoylphosphatidylcholine|3|N||",
        "C0000039|ENG|S|L0000039|VO|S0007564|N|A26634305||M0023172|D015060|MSH|EN|"
        "D015060|1,2 Dipalmitoylphosphatidylcholine|3|N|256|",
        "C0000039|ENG|S|L0000039|VO|S0352885|N|A29142011||M0023172|D015060|MSH|ET|"
        "D015060|1,2-Dipalmitoyl Phosphatidylcholine|3|N|256|",
    ]
    file_stream = io.StringIO("\n".join(content_lines))

    # 2. Act: Parse the file
    records = list(parse_mrconso(file_stream, max_errors=10))

    # 3. Assert
    assert len(records) == 4
    assert records[0] == MrconsoRecord(
        cui="C0000005",
        lat="ENG",
        ts="P",
        lui="L0000005",
        stt="PF",
        sui="S0007492",
        ispref="Y",
        aui="A26634265",
        saui=None,
        scui="M0019694",
        sdui="D012711",
        sab="MSH",
        tty="PEN",
        code="D012711",
        record_str="(131)I-Macroaggregated Albumin",
        srl="0",
        suppress="N",
        cvf="256",
        raw_record=content_lines[0],
    )
    assert records[1].cui == "C0000039"
    assert records[1].raw_record == content_lines[1]
    assert records[3].ispref == "N"
    assert records[3].raw_record == content_lines[3]


def test_parse_mrsty():
    """
    Tests that the parse_mrsty function correctly parses a pipe-delimited
    MRSTY.RRF file stream.
    """
    # 1. Arrange
    content_lines = [
        "C0000052|T029|B1.2.1.2.1|Body Part, Organ, or Organ Component|||",
        "C0000052|T060|B2.2.1.2|Diagnostic Procedure|||",
        "C0000074|T033|B1.2.1.1|Finding|AT12345|CVF123|",  # All fields present
        "C0000074|T047|B2.2.1.2.1|Disease or Syndrome|||",
        "C0000097|T121|B4|Pharmacologic Substance|||",
        "C0000102|T061|B2.2.1.2.2|Therapeutic or Preventive Procedure",  # Malformed row
    ]
    file_stream = io.StringIO("\n".join(content_lines))

    # 2. Act
    records = list(parse_mrsty(file_stream, max_errors=10))

    # 3. Assert
    assert len(records) == 5
    assert records[0] == MrstyRecord(
        cui="C0000052",
        tui="T029",
        stn="B1.2.1.2.1",
        sty="Body Part, Organ, or Organ Component",
        atui=None,
        cvf=None,
        raw_record=content_lines[0],
    )
    assert records[2] == MrstyRecord(
        cui="C0000074",
        tui="T033",
        stn="B1.2.1.1",
        sty="Finding",
        atui="AT12345",
        cvf="CVF123",
        raw_record=content_lines[2],
    )
    assert records[4].cui == "C0000097"
