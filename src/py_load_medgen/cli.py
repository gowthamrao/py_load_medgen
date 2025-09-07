import argparse
import logging
import os
import sys
import uuid
import traceback
from pathlib import Path
from importlib import metadata
from typing import Iterator, TypeVar

from py_load_medgen.downloader import Downloader
from py_load_medgen.loader.factory import LoaderFactory
from py_load_medgen.sql.ddl import STAGING_CONCEPTS_DDL, STAGING_NAMES_DDL
from py_load_medgen.parser import (
    parse_mrconso,
    stream_mrconso_tsv,
    parse_names,
    stream_names_tsv,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Constants ---
NCBI_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
NCBI_FTP_PATH = "/pub/medgen/"

# File and Table Mappings
ETL_CONFIG = [
    (
        "MRCONSO.RRF",
        parse_mrconso,
        stream_mrconso_tsv,
        "staging_medgen_concepts",
        STAGING_CONCEPTS_DDL,
        "medgen_concepts",
        [
            "ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (aui);",
            "CREATE INDEX idx_{table_name}_cui ON {table_name} (cui);",
            "CREATE INDEX idx_{table_name}_sab ON {table_name} (sab);",
            "CREATE INDEX idx_{table_name}_code ON {table_name} (code);",
        ],
    ),
    (
        "NAMES.RRF.gz",
        parse_names,
        stream_names_tsv,
        "staging_medgen_names",
        STAGING_NAMES_DDL,
        "medgen_names",
        [
            "ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (cui, name);",
            "CREATE INDEX idx_{table_name}_cui ON {table_name} (cui);",
        ],
    ),
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
        choices=["full"],
        default="full",
        help="The ETL load strategy to perform.",
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
                remote_file, _, _, _, _, _, _ = config
                local_path = download_dir / Path(remote_file).name
                downloader.download_file(remote_file, local_path, checksums)
                local_file_paths[remote_file] = local_path
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
            )

            for config in ETL_CONFIG:
                (
                    remote_file,
                    parser_func,
                    transformer_func,
                    staging_table,
                    staging_ddl,
                    prod_table,
                    index_ddls,
                ) = config

                local_path = local_file_paths[remote_file]

                logging.info(f"--- Starting ETL for {remote_file} -> {prod_table} ---")

                # A. Initialize Staging
                loader.initialize_staging(staging_table, staging_ddl)

                # B. Parse, Transform, and Load
                logging.info(f"Opening and parsing {local_path}...")
                record_counter = Counter()
                f = None
                try:
                    if remote_file.endswith(".gz"):
                        records_iterator = parser_func(local_path)
                    else:
                        f = open(local_path, "r", encoding="utf-8")
                        records_iterator = parser_func(f)

                    counted_records = count_iterator(records_iterator, record_counter)
                    byte_iterator = transformer_func(counted_records)
                    loader.bulk_load(staging_table, byte_iterator)

                    logging.info(f"Finished loading data into {staging_table}.")
                    logging.info(f"Extracted and loaded {record_counter.value} records.")
                    total_records_extracted += record_counter.value
                    total_records_loaded += record_counter.value

                finally:
                    if f:
                        f.close()

                # C. Apply Changes (Full Load)
                if args.mode == "full":
                    loader.apply_changes(staging_table, prod_table, index_ddls)
                    loader.cleanup(staging_table, prod_table)

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
