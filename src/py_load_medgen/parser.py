import csv
import gzip
import logging
from dataclasses import dataclass, fields
from pathlib import Path
from typing import IO, Iterator, Optional, List, Dict, TypeVar, Type

from py_load_medgen.schemas import (
    MRCONSO_RRF_SCHEMA,
    NAMES_RRF_SCHEMA,
    MEDGEN_HPO_MAPPING_SCHEMA,
    MRREL_RRF_SCHEMA,
    MRSTY_RRF_SCHEMA,
    MRSAT_RRF_SCHEMA,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ParsingError(Exception):
    """Custom exception for errors encountered during file parsing."""

    pass


T = TypeVar("T")


# --- Dataclass Definitions ---
@dataclass(frozen=True)
class MrconsoRecord:
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
    record_str: str
    srl: str
    suppress: str
    cvf: Optional[str]
    raw_record: str


@dataclass(frozen=True)
class MedgenName:
    cui: str
    name: str
    source: str
    suppress: str
    raw_record: str


@dataclass(frozen=True)
class MedgenHpoMapping:
    cui: str
    sdui: str
    hpo_str: str
    medgen_str: str
    medgen_str_sab: str
    sty: str
    raw_record: str


@dataclass(frozen=True)
class MrrelRecord:
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


@dataclass(frozen=True)
class MrstyRecord:
    cui: str
    tui: str
    stn: str
    sty: str
    atui: Optional[str]
    cvf: Optional[str]
    raw_record: str


@dataclass(frozen=True)
class MrsatRecord:
    cui: str
    lui: Optional[str]
    sui: Optional[str]
    metaui: Optional[str]
    stype: str
    code: Optional[str]
    atui: str
    satui: Optional[str]
    atn: str
    sab: str
    atv: Optional[str]
    suppress: str
    cvf: Optional[str]
    raw_record: str


# --- Helper functions ---
def _dataclass_to_tsv(record) -> bytes:
    """Converts a dataclass instance to a UTF-8 encoded TSV line."""
    values = []
    for field in fields(record):
        value = getattr(record, field.name)
        if field.name == "raw_record" and isinstance(value, str):
            value = value.replace("\t", " ").replace("\n", " ")
        values.append(str(value or r"\N"))
    line = "\t".join(values)
    return (line + "\n").encode("utf-8")


def stream_tsv(records: Iterator[T]) -> Iterator[bytes]:
    """Generic function to stream dataclass records as TSV."""
    for record in records:
        yield _dataclass_to_tsv(record)


def _handle_parsing_error(
    error_count: int, max_errors: int, line_num: int, filename: str, message: str
) -> int:
    """Centralized error logging and counting for parsers."""
    logging.warning(f"Skipping malformed row {line_num} in {filename}: {message}")
    error_count += 1
    if error_count > max_errors:
        raise ParsingError(
            f"Exceeded maximum parsing errors ({max_errors}) in {filename}. Aborting."
        )
    return error_count


def _populate_optional_fields(record_dict: Dict, record_class: type) -> Dict:
    """Converts empty strings to None for fields marked as Optional in the dataclass."""
    for f in fields(record_class):
        if f.type == Optional[str] and f.name in record_dict:
            if not record_dict[f.name]:
                record_dict[f.name] = None
    return record_dict


def _parse_pipe_delimited(file_stream: IO[str], schema: List[str], record_class: type, filename: str, max_errors: int) -> Iterator:
    """Generic parser for pipe-delimited files, handling trailing delimiters."""
    error_count = 0
    num_fields = len(schema)

    for i, line in enumerate(file_stream, start=1):
        raw_record = line.rstrip('\r\n')
        processing_line = line.strip()
        if not processing_line:
            continue

        row = next(csv.reader([processing_line], delimiter="|", quoting=csv.QUOTE_NONE))

        if len(row) > num_fields:
            if all(f == "" for f in row[num_fields:]):
                row = row[:num_fields]

        if len(row) != num_fields:
            error_count = _handle_parsing_error(
                error_count, max_errors, i, filename,
                f"expected {num_fields} columns, found {len(row)}"
            )
            continue

        record_dict = dict(zip(schema, (field.strip() for field in row)))
        record_dict["raw_record"] = raw_record
        record_dict = _populate_optional_fields(record_dict, record_class)
        yield record_class(**record_dict)


def parse_mrconso(file_stream: IO[str], max_errors: int) -> Iterator[MrconsoRecord]:
    yield from _parse_pipe_delimited(file_stream, MRCONSO_RRF_SCHEMA, MrconsoRecord, "MRCONSO.RRF", max_errors)


def parse_mrrel(file_stream: IO[str], max_errors: int) -> Iterator[MrrelRecord]:
    yield from _parse_pipe_delimited(file_stream, MRREL_RRF_SCHEMA, MrrelRecord, "MRREL.RRF", max_errors)


def parse_mrsty(file_stream: IO[str], max_errors: int) -> Iterator[MrstyRecord]:
    yield from _parse_pipe_delimited(file_stream, MRSTY_RRF_SCHEMA, MrstyRecord, "MRSTY.RRF", max_errors)


def parse_mrsat(file_stream: IO[str], max_errors: int) -> Iterator[MrsatRecord]:
    yield from _parse_pipe_delimited(file_stream, MRSAT_RRF_SCHEMA, MrsatRecord, "MRSAT.RRF", max_errors)


def parse_names(file_path: Path, max_errors: int) -> Iterator[MedgenName]:
    """
    Parses a gzipped, pipe-delimited NAMES.RRF.gz file with resilience
    to column reordering.
    """
    error_count = 0
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        header = f.readline().strip()
        if header.startswith("#"):
            header = header[1:]

        fieldnames = [h.strip() for h in header.split("|") if h]

        for i, line in enumerate(f, start=2):
            raw_record = line.rstrip('\r\n')
            processing_line = line.strip()
            if not processing_line:
                continue

            reader = csv.DictReader([processing_line], fieldnames=fieldnames, delimiter="|", quoting=csv.QUOTE_NONE)

            try:
                record_dict = next(reader)
            except StopIteration:
                continue

            if None in record_dict.values():
                error_count = _handle_parsing_error(
                    error_count, max_errors, i, "NAMES.RRF",
                    f"incorrect number of columns. Expected {len(fieldnames)}."
                )
                continue

            try:
                # Filter out None keys which can be added by DictReader for extra values
                normalized_dict = {k.lower(): v for k, v in record_dict.items() if k is not None}
                normalized_dict["raw_record"] = raw_record
                yield MedgenName(**normalized_dict)
            except TypeError:
                 error_count = _handle_parsing_error(
                    error_count, max_errors, i, "NAMES.RRF",
                    f"mismatched columns. Expected { {f.name for f in fields(MedgenName) if f.name != 'raw_record'} }. Got: {set(normalized_dict.keys())}"
                )
                 continue


def parse_hpo_mapping(file_path: Path, max_errors: int) -> Iterator[MedgenHpoMapping]:
    """Parses a gzipped, tab-delimited MedGen_HPO_Mapping.txt.gz file."""
    error_count = 0
    schema = MEDGEN_HPO_MAPPING_SCHEMA
    num_fields = len(schema)

    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        header = f.readline()
        if not header.lower().startswith(("#cui", "cui")):
            f.seek(0)

        for i, line in enumerate(f, start=1):
            raw_record = line.rstrip('\r\n')
            processing_line = line.strip()
            if not processing_line:
                continue

            row = next(csv.reader([processing_line], delimiter="\t", quoting=csv.QUOTE_NONE))

            if len(row) != num_fields:
                error_count = _handle_parsing_error(
                    error_count, max_errors, i, "HPO_MAPPING",
                    f"expected {num_fields} columns, found {len(row)}"
                )
                continue

            record_dict = dict(zip(schema, (field.strip() for field in row)))
            record_dict["raw_record"] = raw_record
            yield MedgenHpoMapping(**record_dict)
