import argparse
import logging
import os
import sys
import traceback
import uuid
from importlib import metadata
from pathlib import Path
from typing import Iterator, TypeVar, TypedDict, Callable, Any, NotRequired

from py_load_medgen.downloader import Downloader
from py_load_medgen.loader.factory import LoaderFactory
from py_load_medgen.parser import (
    parse_hpo_mapping,
    parse_mrconso,
    parse_mrrel,
    parse_mrsat,
    parse_mrsty,
    parse_names,
    stream_hpo_mapping_tsv,
    stream_mrconso_tsv,
    stream_mrrel_tsv,
    stream_mrsat_tsv,
    stream_mrsty_tsv,
    stream_names_tsv,
)
from py_load_medgen.sql.ddl import (
    PRODUCTION_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_INDEXES_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
    PRODUCTION_MEDGEN_RELATIONSHIPS_DDL,
    PRODUCTION_MEDGEN_RELATIONSHIPS_INDEXES_DDL,
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
    PRODUCTION_SEMANTIC_TYPES_DDL,
    PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
    STAGING_CONCEPTS_DDL,
    STAGING_MEDGEN_HPO_MAPPING_DDL,
    STAGING_MEDGEN_RELATIONSHIPS_DDL,
    STAGING_NAMES_DDL,
    STAGING_SEMANTIC_TYPES_DDL,
    STAGING_MEDGEN_SOURCES_DDL,
    PRODUCTION_MEDGEN_SOURCES_DDL,
    PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constants ---
NCBI_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
NCBI_FTP_PATH = "/pub/medgen/"

# --- Type Definitions for ETL Configuration ---
class EtlFileConfig(TypedDict):
    file: str
    parser: Callable[[Any], Iterator[Any]]
    transformer: Callable[[Iterator[Any]], Iterator[bytes]]
    staging_table: str
    staging_ddl: str
    prod_table: str
    prod_ddl: str
    prod_pk: str
    business_key: str
    index_ddls: list[str]
    full_load_select_sql: NotRequired[str]


# --- File and Table Mappings ---
ETL_CONFIG: list[EtlFileConfig] = [
    {
        "file": "MRCONSO.RRF",
        "parser": parse_mrconso,
        "transformer": stream_mrconso_tsv,
        "staging_table": "staging_medgen_concepts",
        "staging_ddl": STAGING_CONCEPTS_DDL,
        "prod_table": "medgen_concepts",
        "prod_ddl": PRODUCTION_CONCEPTS_DDL,
        "prod_pk": "concept_id",
        "business_key": "aui",
        "index_ddls": PRODUCTION_CONCEPTS_INDEXES_DDL,
    },
    {
        "file": "MRSTY.RRF",
        "parser": parse_mrsty,
        "transformer": stream_mrsty_tsv,
        "staging_table": "staging_medgen_semantic_types",
        "staging_ddl": STAGING_SEMANTIC_TYPES_DDL,
        "prod_table": "medgen_semantic_types",
        "prod_ddl": PRODUCTION_SEMANTIC_TYPES_DDL,
        "prod_pk": "semantic_type_id",
        "business_key": "atui",
        "index_ddls": PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
    },
    {
        "file": "MRREL.RRF",
        "parser": parse_mrrel,
        "transformer": stream_mrrel_tsv,
        "staging_table": "staging_medgen_relationships",
        "staging_ddl": STAGING_MEDGEN_RELATIONSHIPS_DDL,
        "prod_table": "medgen_relationships",
        "prod_ddl": PRODUCTION_MEDGEN_RELATIONSHIPS_DDL,
        "prod_pk": "relationship_id",
        "business_key": "rui",
        "index_ddls": PRODUCTION_MEDGEN_RELATIONSHIPS_INDEXES_DDL,
    },
    {
        "file": "MRSAT.RRF",
        "parser": parse_mrsat,
        "transformer": stream_mrsat_tsv,
        "staging_table": "staging_medgen_sources",
        "staging_ddl": STAGING_MEDGEN_SOURCES_DDL,
        "prod_table": "medgen_sources",
        "prod_ddl": PRODUCTION_MEDGEN_SOURCES_DDL,
        "prod_pk": "source_id",
        "business_key": "atui",
        "index_ddls": PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
        "full_load_select_sql": "INSERT INTO {new_production_table} (cui, source_abbreviation, attribute_name, attribute_value, raw_record) SELECT cui, sab, atn, atv, raw_record FROM {staging_table};"
    },
    {
        "file": "NAMES.RRF.gz",
        "parser": parse_names,
        "transformer": stream_names_tsv,
        "staging_table": "staging_medgen_names",
        "staging_ddl": STAGING_NAMES_DDL,
        "prod_table": "medgen_names",
        "prod_ddl": PRODUCTION_NAMES_DDL,
        "prod_pk": "name_id",
        "business_key": "name",
        "index_ddls": PRODUCTION_NAMES_INDEXES_DDL,
    },
    {
        "file": "MedGen_HPO_Mapping.txt.gz",
        "parser": parse_hpo_mapping,
        "transformer": stream_hpo_mapping_tsv,
        "staging_table": "staging_medgen_hpo_mapping",
        "staging_ddl": STAGING_MEDGEN_HPO_MAPPING_DDL,
        "prod_table": "medgen_hpo_mapping",
        "prod_ddl": PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
        "prod_pk": "hpo_mapping_id",
        "business_key": "sdui", # Assuming SDUI (source ID) is a stable business key
        "index_ddls": PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
    },
]


# --- Helper for counting records in a stream ---
T = TypeVar("T")


class Counter:
    value: int = 0


def count_iterator(iterator: Iterator[T], counter: Counter) -> Iterator[T]:
    """Wraps an iterator to count the number of items yielded."""
    for item in iterator:
        counter.value += 1
        yield item


def main():
    """Main CLI entry point for the MedGen ETL tool."""
    parser = argparse.ArgumentParser(
        description="A CLI tool for loading NCBI MedGen data into a database."
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default=".",
        help="The directory to download the MedGen files to. Defaults to the current directory.",
    )
    parser.add_argument(
        "--db-dsn",
        type=str,
        default=os.environ.get("MEDGEN_DB_DSN"),
        help="PostgreSQL connection string (DSN). Can also be set via the MEDGEN_DB_DSN environment variable.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "delta"],
        default="full",
        help="The ETL load strategy to perform. 'full' performs a complete refresh. 'delta' applies changes since the last load.",
    )
    parser.add_argument(
        "--max-parse-errors",
        type=int,
        default=100,
        help="The maximum number of parsing errors to tolerate before aborting the ETL process.",
    )

    args = parser.parse_args()

    if not args.db_dsn:
        logging.error(
            "Database connection string is required. Please provide it via --db-dsn or MEDGEN_DB_DSN."
        )
        sys.exit(1)

    # --- Setup for Logging and Execution ---
    run_id = uuid.uuid4()
    try:
        pkg_version = metadata.version("py-load-medgen")
    except metadata.PackageNotFoundError:
        pkg_version = "0.0.0-dev"

    logging.info(f"Starting MedGen ETL run ID: {run_id}")
    logging.info(f"Package version: {pkg_version}, Mode: {args.mode}")

    download_dir = Path(args.download_dir)
    download_dir.mkdir(exist_ok=True)
    local_file_paths = {}
    source_file_checksums = {}

    # --- Download Phase ---
    try:
        with Downloader(
            ftp_host=NCBI_FTP_HOST, ftp_path=NCBI_FTP_PATH
        ) as downloader:
            # Get release version from README
            release_version = downloader.get_release_version()
            logging.info(f"MedGen Release Version: {release_version}")

            ftp_files = downloader.list_files()
            checksum_file = next((f for f in ftp_files if "md5" in f.lower()), None)
            checksums = {}
            if checksum_file:
                logging.info(f"Found checksum file: {checksum_file}")
                checksums = downloader.get_checksums(checksum_file)
            else:
                logging.warning(
                    "No checksum file found. Skipping file integrity verification."
                )

            for config in ETL_CONFIG:
                remote_file = config["file"]
                local_path = download_dir / Path(remote_file).name
                downloader.download_file(remote_file, local_path, checksums)
                local_file_paths[remote_file] = local_path
                # Prepare source file info for logging
                source_file_checksums[remote_file] = checksums.get(remote_file)

    except Exception as e:
        logging.error(f"Failed during download phase: {e}", exc_info=True)
        sys.exit(1)

    # --- ETL Phase ---
    log_id = None
    total_records_extracted = 0
    total_records_loaded = 0

    try:
        with LoaderFactory.create_loader(db_dsn=args.db_dsn) as loader:
            # 1. Log the start of the run
            log_id = loader.log_run_start(
                run_id=run_id,
                package_version=pkg_version,
                load_mode=args.mode,
                source_files=source_file_checksums,
                medgen_release_version=release_version,
            )

            for config in ETL_CONFIG:
                local_path = local_file_paths[config["file"]]

                logging.info(f"--- Starting ETL for {config['file']} -> {config['prod_table']} ---")

                # A. Initialize Staging
                loader.initialize_staging(config["staging_table"], config["staging_ddl"])

                # B. Parse, Transform, and Load
                logging.info(f"Opening and parsing {local_path}...")
                record_counter = Counter()
                f = None
                try:
                    parser_func = config["parser"]
                    if config["file"].endswith(".gz"):
                        # For gzipped files, the parser function takes the path
                        records_iterator = parser_func(local_path, max_errors=args.max_parse_errors)
                    else:
                        # For plain text files, the parser takes a file stream
                        f = open(local_path, "r", encoding="utf-8")
                        records_iterator = parser_func(f, max_errors=args.max_parse_errors)

                    counted_records = count_iterator(records_iterator, record_counter)
                    byte_iterator = config["transformer"](counted_records)
                    loader.bulk_load(config["staging_table"], byte_iterator)

                    logging.info(f"Finished loading data into {config['staging_table']}.")
                    logging.info(f"Extracted and loaded {record_counter.value} records.")
                    total_records_extracted += record_counter.value
                    total_records_loaded += record_counter.value

                finally:
                    if f:
                        f.close()

                # C. Apply Changes based on mode and log details
                if args.mode == "full":
                    loader.apply_changes(
                        mode="full",
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        production_ddl=config["prod_ddl"],
                        index_ddls=config["index_ddls"],
                        pk_name=config["prod_pk"],
                        full_load_select_sql=config.get("full_load_select_sql"),
                    )
                    # For full loads, extracted equals inserted
                    metrics = {
                        "table_name": config["prod_table"],
                        "records_extracted": record_counter.value,
                        "records_inserted": record_counter.value,
                        "records_deleted": 0,
                        "records_updated": 0,
                    }
                    loader.log_run_detail(log_id, metrics)

                elif args.mode == "delta":
                    cdc_metrics = loader.execute_cdc(
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        pk_name=config["prod_pk"],
                        business_key=config["business_key"],
                    )
                    # For delta loads, log CDC results
                    metrics = {
                        "table_name": config["prod_table"],
                        "records_extracted": record_counter.value,
                        "records_inserted": cdc_metrics.get("inserts", 0),
                        "records_deleted": cdc_metrics.get("deletes", 0),
                        "records_updated": cdc_metrics.get("updates", 0),
                    }
                    loader.log_run_detail(log_id, metrics)
                    loader.apply_changes(
                        mode="delta",
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        production_ddl=config["prod_ddl"],
                        index_ddls=config["index_ddls"],
                        pk_name=config["prod_pk"],
                        business_key=config["business_key"],
                    )

                loader.cleanup(config["staging_table"], config["prod_table"])

            # 4. Log success if we reached the end
            loader.log_run_finish(
                log_id,
                status="Succeeded",
                records_extracted=total_records_extracted,
                records_loaded=total_records_loaded,
            )
            logging.info("--- ETL process finished successfully. ---")

    except Exception as e:
        logging.error(
            f"An unexpected error occurred during the ETL process: {e}", exc_info=True
        )
        # 5. Log failure if an exception occurred
        if log_id is not None:
            # Use a new loader connection in case the old one is broken
            try:
                with LoaderFactory.create_loader(db_dsn=args.db_dsn) as error_loader:
                    error_loader.log_run_finish(
                        log_id,
                        status="Failed",
                        records_extracted=total_records_extracted,
                        records_loaded=total_records_loaded,
                        error_message=traceback.format_exc(),
                    )
            except Exception as log_e:
                logging.critical(f"Failed to log the ETL failure: {log_e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
