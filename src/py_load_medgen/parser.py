from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MrconsoRecord:
    """
    Represents a single record from the MRCONSO.RRF file.
    Field names correspond to the columns defined in the UMLS Reference Manual.
    See: https://www.ncbi.nlm.nih.gov/books/NBK9685/
    """

    cui: str
    lat: str
    ts: str
    lui: str
    stt: str
    sui: str
    ispref: str
    aui: str
    saui: Optional[str]
    scui: Optional[str]
    sdui: Optional[str]
    sab: str
    tty: str
    code: str
    str: str
    srl: str
    suppress: str
    cvf: Optional[str]
    raw_record: str


import csv
import logging
from pathlib import Path
from typing import IO, Iterator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


import gzip
import io
from dataclasses import fields


@dataclass(frozen=True)
class MedgenName:
    """Represents a single record from the NAMES.RRF file."""

    cui: str
    name: str
    source: str
    suppress: str
    raw_record: str


@dataclass(frozen=True)
class MedgenHpoMapping:
    """Represents a single record from the MedGen_HPO_Mapping.txt.gz file."""

    cui: str
    sdui: str
    hpo_str: str
    medgen_str: str
    medgen_str_sab: str
    sty: str
    raw_record: str


def stream_mrconso_tsv(records: Iterator[MrconsoRecord]) -> Iterator[bytes]:
    """
    Transforms an iterator of MrconsoRecord objects into a streaming iterator
    of UTF-8 encoded TSV lines.
    """
    for record in records:
        line = "\t".join(
            str(getattr(record, field.name) or r"\N") for field in fields(record)
        )
        yield (line + "\n").encode("utf-8")


def stream_names_tsv(records: Iterator[MedgenName]) -> Iterator[bytes]:
    """
    Transforms an iterator of MedgenName objects into a streaming iterator
    of UTF-8 encoded TSV lines.
    """
    for record in records:
        line = "\t".join(
            str(getattr(record, field.name) or r"\N") for field in fields(record)
        )
        yield (line + "\n").encode("utf-8")


def stream_hpo_mapping_tsv(records: Iterator[MedgenHpoMapping]) -> Iterator[bytes]:
    """
    Transforms an iterator of MedgenHpoMapping objects into a streaming iterator
    of UTF-8 encoded TSV lines.
    """
    for record in records:
        line = "\t".join(
            str(getattr(record, field.name) or r"\N") for field in fields(record)
        )
        yield (line + "\n").encode("utf-8")


def parse_mrconso(file_stream: IO[str]) -> Iterator[MrconsoRecord]:
    """
    Parses a pipe-delimited MRCONSO.RRF file stream.
    Args:
        file_stream: A text file-like object containing MRCONSO.RRF data.
    Yields:
        MrconsoRecord instances for each valid row in the file.
    """
    # The RRF format is pipe-delimited, and each row ends with a pipe.
    # We can't use the csv module directly with the file_stream iterator
    # because we need to preserve the raw line.
    for i, line in enumerate(file_stream):
        raw_line = line.strip()
        if not raw_line:
            continue

        row = raw_line.split("|")
        # After splitting, a valid row will have 19 elements, with the last one being empty.
        if len(row) < 18:
            logging.warning(
                f"Skipping malformed row {i+1}: expected 18 columns, found {len(row) - 1}"
            )
            continue

        # Unpack the row into the dataclass fields.
        # Optional fields (saui, scui, sdui, cvf) are empty strings in the file if null.
        # We convert them to None for type consistency.
        try:
            yield MrconsoRecord(
                cui=row[0],
                lat=row[1],
                ts=row[2],
                lui=row[3],
                stt=row[4],
                sui=row[5],
                ispref=row[6],
                aui=row[7],
                saui=row[8] if row[8] else None,
                scui=row[9] if row[9] else None,
                sdui=row[10] if row[10] else None,
                sab=row[11],
                tty=row[12],
                code=row[13],
                str=row[14],
                srl=row[15],
                suppress=row[16],
                cvf=row[17] if row[17] else None,
                raw_record=raw_line,
            )
        except IndexError:
            logging.warning(f"Skipping malformed row {i+1}: not enough columns.")


def parse_names(file_path: Path) -> Iterator[MedgenName]:
    """
    Parses a gzipped, pipe-delimited NAMES.RRF.gz file.
    Args:
        file_path: Path to the gzipped file.
    Yields:
        MedgenName instances for each valid row in the file.
    """
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        # Skip header
        header = f.readline()
        if not header.startswith("#CUI"):
            logging.warning("NAMES.RRF file does not have the expected header.")

        for i, line in enumerate(f):
            raw_line = line.strip()
            if not raw_line:
                continue

            row = raw_line.split("|")
            # Each row should have 5 elements, with the last one being empty.
            if len(row) < 5:
                logging.warning(
                    f"Skipping malformed row {i+1} in NAMES.RRF: expected 4 columns, found {len(row) - 1}"
                )
                continue

            yield MedgenName(
                cui=row[0],
                name=row[1],
                source=row[2],
                suppress=row[3],
                raw_record=raw_line,
            )


def parse_hpo_mapping(file_path: Path) -> Iterator[MedgenHpoMapping]:
    """
    Parses a gzipped, tab-delimited MedGen_HPO_Mapping.txt.gz file.
    Args:
        file_path: Path to the gzipped file.
    Yields:
        MedgenHpoMapping instances for each valid row in the file.
    """
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        # The README implies a header, but doesn't show it. We'll check for it.
        # It's safer to read the first line and check if it looks like a header.
        first_line = f.readline()
        if not first_line.lower().startswith("#cui") and not first_line.lower().startswith("cui"):
            # If it doesn't look like a header, process it as a data line
            f.seek(0)

        for i, line in enumerate(f):
            raw_line = line.strip()
            if not raw_line:
                continue

            # This file is tab-delimited
            row = raw_line.split("\t")
            if len(row) != 6:
                logging.warning(
                    f"Skipping malformed row {i+1} in HPO Mapping file: "
                    f"expected 6 columns, found {len(row)}"
                )
                continue

            yield MedgenHpoMapping(
                cui=row[0],
                sdui=row[1],
                hpo_str=row[2],
                medgen_str=row[3],
                medgen_str_sab=row[4],
                sty=row[5],
                raw_record=raw_line,
            )


@dataclass(frozen=True)
class MrrelRecord:
    """
    Represents a single record from the MRREL.RRF file.
    Field names correspond to the columns defined in the UMLS Reference Manual.
    See: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/
    """

    cui1: str
    aui1: Optional[str]
    stype1: str
    rel: str
    cui2: str
    aui2: Optional[str]
    stype2: str
    rela: Optional[str]
    rui: Optional[str]
    srui: Optional[str]
    sab: str
    sl: Optional[str]
    rg: Optional[str]
    dir: Optional[str]
    suppress: str
    cvf: Optional[str]
    raw_record: str


def stream_mrrel_tsv(records: Iterator[MrrelRecord]) -> Iterator[bytes]:
    """
    Transforms an iterator of MrrelRecord objects into a streaming iterator
    of UTF-8 encoded TSV lines.
    """
    for record in records:
        line = "\t".join(
            str(getattr(record, field.name) or r"\N") for field in fields(record)
        )
        yield (line + "\n").encode("utf-8")


def parse_mrrel(file_stream: IO[str]) -> Iterator[MrrelRecord]:
    """
    Parses a pipe-delimited MRREL.RRF file stream.
    Args:
        file_stream: A text file-like object containing MRREL.RRF data.
    Yields:
        MrrelRecord instances for each valid row in the file.
    """
    for i, line in enumerate(file_stream):
        raw_line = line.strip()
        if not raw_line:
            continue

        row = raw_line.split("|")
        # After splitting, a valid row will have 17 elements, with the last one being empty.
        if len(row) < 16:
            logging.warning(
                f"Skipping malformed row {i+1} in MRREL.RRF: expected 16 columns, found {len(row) - 1}"
            )
            continue

        try:
            yield MrrelRecord(
                cui1=row[0],
                aui1=row[1] if row[1] else None,
                stype1=row[2],
                rel=row[3],
                cui2=row[4],
                aui2=row[5] if row[5] else None,
                stype2=row[6],
                rela=row[7] if row[7] else None,
                rui=row[8] if row[8] else None,
                srui=row[9] if row[9] else None,
                sab=row[10],
                sl=row[11] if row[11] else None,
                rg=row[12] if row[12] else None,
                dir=row[13] if row[13] else None,
                suppress=row[14],
                cvf=row[15] if row[15] else None,
                raw_record=raw_line,
            )
        except IndexError:
            logging.warning(f"Skipping malformed row {i+1} in MRREL.RRF: not enough columns.")


@dataclass(frozen=True)
class MrstyRecord:
    """
    Represents a single record from the MRSTY.RRF file.
    Field names correspond to the columns defined in the UMLS Reference Manual.
    See: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.Tf/
    """

    cui: str
    tui: str
    stn: str
    sty: str
    atui: Optional[str]
    cvf: Optional[str]
    raw_record: str


def stream_mrsty_tsv(records: Iterator[MrstyRecord]) -> Iterator[bytes]:
    """
    Transforms an iterator of MrstyRecord objects into a streaming iterator
    of UTF-8 encoded TSV lines.
    """
    for record in records:
        line = "\t".join(
            str(getattr(record, field.name) or r"\N") for field in fields(record)
        )
        yield (line + "\n").encode("utf-8")


def parse_mrsty(file_stream: IO[str]) -> Iterator[MrstyRecord]:
    """
    Parses a pipe-delimited MRSTY.RRF file stream.
    Args:
        file_stream: A text file-like object containing MRSTY.RRF data.
    Yields:
        MrstyRecord instances for each valid row in the file.
    """
    for i, line in enumerate(file_stream):
        raw_line = line.strip()
        if not raw_line:
            continue

        row = raw_line.split("|")
        # After splitting, a valid row will have 7 elements, with the last one being empty.
        if len(row) < 6:
            logging.warning(
                f"Skipping malformed row {i+1} in MRSTY.RRF: expected 6 columns, found {len(row) - 1}"
            )
            continue

        try:
            yield MrstyRecord(
                cui=row[0],
                tui=row[1],
                stn=row[2],
                sty=row[3],
                atui=row[4] if row[4] else None,
                cvf=row[5] if row[5] else None,
                raw_record=raw_line,
            )
        except IndexError:
            logging.warning(f"Skipping malformed row {i+1} in MRSTY.RRF: not enough columns.")
