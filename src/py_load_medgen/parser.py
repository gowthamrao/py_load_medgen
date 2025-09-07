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


import csv
import logging
from pathlib import Path
from typing import IO, Iterator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


import io
from dataclasses import fields

def records_to_tsv(records: Iterator[MrconsoRecord]) -> io.StringIO:
    """Converts an iterator of MrconsoRecord objects to a TSV in-memory file."""
    buffer = io.StringIO()
    # Note: The order of fields in the dataclass must match the table schema.
    for record in records:
        line = "\t".join(
            # Convert None to the string \N for PostgreSQL's COPY command
            str(getattr(record, field.name) or r"\N")
            for field in fields(record)
        )
        buffer.write(line + "\n")
    buffer.seek(0)
    return buffer


def parse_mrconso(file_stream: IO[str]) -> Iterator[MrconsoRecord]:
    """
    Parses a pipe-delimited MRCONSO.RRF file stream.

    Args:
        file_stream: A text file-like object containing MRCONSO.RRF data.

    Yields:
        MrconsoRecord instances for each valid row in the file.
    """
    # The RRF format is pipe-delimited, and each row ends with a pipe.
    reader = csv.reader(file_stream, delimiter="|", quotechar="\\")
    for i, row in enumerate(reader):
        # After splitting, a valid row will have 19 elements, with the last one being empty.
        if len(row) < 18:
            logging.warning(f"Skipping malformed row {i+1}: expected 18 columns, found {len(row) - 1}")
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
            )
        except IndexError:
            logging.warning(f"Skipping malformed row {i+1}: not enough columns.")
