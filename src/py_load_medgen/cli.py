# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import argparse
import logging
import os
import sys
import traceback
import uuid
from importlib import metadata
from pathlib import Path
import sys
from typing import Any, Callable, Iterator, TypedDict, TypeVar

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired

from py_load_medgen.downloader import ChecksumsNotFoundError, Downloader
from py_load_medgen.loader.factory import LoaderFactory
from py_load_medgen.parser import (
    parse_hpo_mapping,
    parse_mrconso,
    parse_mrrel,
    parse_mrsat,
    parse_mrsty,
    parse_names,
    stream_tsv,
)
from py_load_medgen.sql.ddl import (
    PRODUCTION_CONCEPTS_DDL,
    PRODUCTION_CONCEPTS_INDEXES_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
    PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
    PRODUCTION_MEDGEN_RELATIONSHIPS_DDL,
    PRODUCTION_MEDGEN_RELATIONSHIPS_INDEXES_DDL,
    PRODUCTION_MEDGEN_SOURCES_DDL,
    PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
    PRODUCTION_NAMES_DDL,
    PRODUCTION_NAMES_INDEXES_DDL,
    PRODUCTION_SEMANTIC_TYPES_DDL,
    PRODUCTION_SEMANTIC_TYPES_INDEXES_DDL,
    STAGING_CONCEPTS_DDL,
    STAGING_MEDGEN_HPO_MAPPING_DDL,
    STAGING_MEDGEN_RELATIONSHIPS_DDL,
    STAGING_MEDGEN_SOURCES_DDL,
    STAGING_NAMES_DDL,
    STAGING_SEMANTIC_TYPES_DDL,
)

from py_load_medgen.logging import JsonFormatter

def setup_logging():
    """
    Configures logging based on the LOG_FORMAT environment variable.
    Defaults to standard text-based logging. If LOG_FORMAT=json, uses
    structured JSON logging.
    """
    log_format = os.environ.get("LOG_FORMAT", "text").lower()

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create a new handler
    handler = logging.StreamHandler(sys.stdout)

    if log_format == "json":
        formatter = JsonFormatter()
        handler.setFormatter(formatter)
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

    root_logger.addHandler(handler)

# Configure logging
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )

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
        "transformer": stream_tsv,
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
        "transformer": stream_tsv,
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
        "transformer": stream_tsv,
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
        "transformer": stream_tsv,
        "staging_table": "staging_medgen_sources",
        "staging_ddl": STAGING_MEDGEN_SOURCES_DDL,
        "prod_table": "medgen_sources",
        "prod_ddl": PRODUCTION_MEDGEN_SOURCES_DDL,
        "prod_pk": "source_id",
        "business_key": "atui",
        "index_ddls": PRODUCTION_MEDGEN_SOURCES_INDEXES_DDL,
        "full_load_select_sql": "INSERT INTO {new_production_table} "
        "(cui, source_abbreviation, attribute_name, attribute_value, raw_record) "
        "SELECT cui, sab, atn, atv, raw_record FROM {staging_table};",
    },
    {
        "file": "NAMES.RRF.gz",
        "parser": parse_names,
        "transformer": stream_tsv,
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
        "transformer": stream_tsv,
        "staging_table": "staging_medgen_hpo_mapping",
        "staging_ddl": STAGING_MEDGEN_HPO_MAPPING_DDL,
        "prod_table": "medgen_hpo_mapping",
        "prod_ddl": PRODUCTION_MEDGEN_HPO_MAPPING_DDL,
        "prod_pk": "hpo_mapping_id",
        "business_key": "sdui", # Assuming SDUI (source ID) is a stable business key
        "index_ddls": PRODUCTION_MEDGEN_HPO_MAPPING_INDEXES_DDL,
    },
]


def main():
    """Main CLI entry point for the MedGen ETL tool."""
    setup_logging()
    parser = argparse.ArgumentParser(
        description="A CLI tool for loading NCBI MedGen data into a database."
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default=".",
        help="The directory to download the MedGen files to. "
        "Defaults to the current directory.",
    )
    parser.add_argument(
        "--db-dsn",
        type=str,
        default=os.environ.get("MEDGEN_DB_DSN"),
        help="PostgreSQL connection string (DSN). "
        "Can also be set via the MEDGEN_DB_DSN environment variable.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "delta"],
        default="full",
        help="The ETL load strategy to perform. 'full' performs a complete refresh. "
        "'delta' applies changes since the last load.",
    )
    parser.add_argument(
        "--max-parse-errors",
        type=int,
        default=100,
        help="The maximum number of parsing errors to tolerate "
        "before aborting the ETL process.",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip file integrity verification. Use this if the FTP server "
        "does not provide a checksums file.",
    )

    args = parser.parse_args()

    if not args.db_dsn:
        logging.error(
            "Database connection string is required. "
            "Please provide it via --db-dsn or MEDGEN_DB_DSN."
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
            release_version = downloader.get_release_version()
            logging.info(f"MedGen Release Version: {release_version}")

            checksums = None
            if not args.no_verify:
                try:
                    # Try to find the checksum file automatically.
                    # Common names are md5sum.txt or MD5SUMS
                    ftp_files = downloader.list_files()
                    checksum_filename = next(
                        (f for f in ftp_files if "md5" in f.lower()), "md5sum.txt"
                    )
                    logging.info(f"Attempting to use checksum file: {checksum_filename}")
                    checksums = downloader.get_checksums(checksum_filename)
                except ChecksumsNotFoundError as e:
                    logging.error(f"Checksum verification failed: {e}")
                    sys.exit(1)
            else:
                logging.warning(
                    "Running with --no-verify. File integrity will not be checked."
                )

            for config in ETL_CONFIG:
                remote_file = config["file"]
                local_path = download_dir / Path(remote_file).name
                downloader.download_file(remote_file, local_path, checksums)
                local_file_paths[remote_file] = local_path
                # Prepare source file info for logging
                if checksums and remote_file in checksums:
                    source_file_checksums[remote_file] = checksums.get(remote_file)

    except (Exception, ChecksumsNotFoundError) as e:
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
                table_metrics = {
                    "table_name": config["prod_table"],
                    "records_extracted": 0,
                    "records_inserted": 0,
                    "records_updated": 0,
                    "records_deleted": 0,
                }

                logging.info(
                    f"--- Starting ETL for {config['file']} -> "
                    f"{config['prod_table']} ---"
                )

                # 1. Initialize Staging
                loader.initialize_staging(
                    config["staging_table"], config["staging_ddl"]
                )

                # 2. Parse, Transform, and Load into Staging
                logging.info(f"Opening and parsing {local_path}...")
                f = None
                try:
                    parser_func = config["parser"]
                    # Determine how to open the file (gzipped or plain text)
                    if config["file"].endswith(".gz"):
                        records_iterator = parser_func(
                            local_path, max_errors=args.max_parse_errors
                        )
                    else:
                        f = open(local_path, "r", encoding="utf-8")
                        records_iterator = parser_func(
                            f, max_errors=args.max_parse_errors
                        )

                    # Transform records to bytes and load, capturing the count
                    byte_iterator = config["transformer"](records_iterator)
                    extracted_count = loader.bulk_load(
                        config["staging_table"], byte_iterator
                    )
                    table_metrics["records_extracted"] = extracted_count
                    total_records_extracted += extracted_count
                    logging.info(
                        f"Extracted and staged {extracted_count} records."
                    )
                finally:
                    if f:
                        f.close()

                # 3. Apply Changes to Production and Capture Metrics
                apply_metrics = {}
                if args.mode == "full":
                    apply_metrics = loader.apply_changes(
                        mode="full",
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        production_ddl=config["prod_ddl"],
                        index_ddls=config["index_ddls"],
                        pk_name=config["prod_pk"],
                        full_load_select_sql=config.get("full_load_select_sql"),
                    )
                elif args.mode == "delta":
                    # First, run CDC to identify changes
                    loader.execute_cdc(
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        pk_name=config["prod_pk"],
                        business_key=config["business_key"],
                    )
                    # Then, apply the identified changes
                    apply_metrics = loader.apply_changes(
                        mode="delta",
                        staging_table=config["staging_table"],
                        production_table=config["prod_table"],
                        production_ddl=config["prod_ddl"],
                        index_ddls=config["index_ddls"],
                        pk_name=config["prod_pk"],
                        business_key=config["business_key"],
                    )

                # 4. Update and Log Detailed Metrics
                table_metrics["records_inserted"] = apply_metrics.get("inserted", 0)
                table_metrics["records_updated"] = apply_metrics.get("updated", 0)
                table_metrics["records_deleted"] = apply_metrics.get("deleted", 0)
                loader.log_run_detail(log_id, table_metrics)

                # 5. Aggregate Total Loaded Records for the Final Summary
                # "Loaded" means a record is new or changed in the production table.
                total_records_loaded += table_metrics["records_inserted"]
                total_records_loaded += table_metrics["records_updated"]

                # 6. Cleanup Staging and Backup Tables
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
