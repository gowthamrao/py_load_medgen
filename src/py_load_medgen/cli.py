import argparse
import logging
import os
import sys
from pathlib import Path

from py_load_medgen.downloader import Downloader
from py_load_medgen.loader.postgres import (
    PostgresNativeLoader,
    STAGING_CONCEPTS_DDL,
    STAGING_NAMES_DDL,
)
from py_load_medgen.parser import (
    parse_mrconso,
    records_to_tsv,
    parse_names,
    names_records_to_tsv,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Constants ---
NCBI_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
NCBI_FTP_PATH = "/pub/medgen/"

# File and Table Mappings
# A list of tuples, where each tuple contains:
# (remote_filename, parser_func, transformer_func, staging_table_name, staging_ddl, production_table_name, index_ddls)
ETL_CONFIG = [
    (
        "MRCONSO.RRF",
        parse_mrconso,
        records_to_tsv,
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
        names_records_to_tsv,
        "staging_medgen_names",
        STAGING_NAMES_DDL,
        "medgen_names",
        [
            "ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (cui, name);",
            "CREATE INDEX idx_{table_name}_cui ON {table_name} (cui);",
        ],
    ),
]


def main():
    """Main CLI entry point for the MedGen ETL tool."""
    parser = argparse.ArgumentParser(description="A CLI tool for loading NCBI MedGen data into a database.")
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
        logging.error("Database connection string is required. Please provide it via --db-dsn or MEDGEN_DB_DSN.")
        sys.exit(1)

    download_dir = Path(args.download_dir)
    download_dir.mkdir(exist_ok=True)
    local_file_paths = {}

    # --- Download Phase ---
    try:
        with Downloader(ftp_host=NCBI_FTP_HOST, ftp_path=NCBI_FTP_PATH) as downloader:
            ftp_files = downloader.list_files()
            checksum_file = next((f for f in ftp_files if "md5" in f.lower()), None)
            checksums = {}
            if checksum_file:
                logging.info(f"Found checksum file: {checksum_file}")
                checksums = downloader.get_checksums(checksum_file)
            else:
                logging.warning("No checksum file found. Skipping file integrity verification.")

            for config in ETL_CONFIG:
                remote_file, _, _, _, _, _, _ = config
                local_path = download_dir / Path(remote_file).name
                downloader.download_file(remote_file, local_path, checksums)
                local_file_paths[remote_file] = local_path

    except Exception as e:
        logging.error(f"Failed during download phase: {e}", exc_info=True)
        sys.exit(1)

    # --- ETL Phase ---
    try:
        with PostgresNativeLoader(db_dsn=args.db_dsn) as loader:
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

                # 1. Initialize Staging
                loader.initialize_staging(staging_table, staging_ddl)

                # 2. Parse, Transform, and Load
                logging.info(f"Opening and parsing {local_path}...")
                # Note: This is not ideal for memory usage. A better approach would be
                # to stream from the parser directly to the loader without creating an
                # intermediate file. For now, this is a direct translation of the
                # previous logic. The parser needs to be refactored to handle streams.
                if remote_file.endswith(".gz"):
                    records_iterator = parser_func(local_path)
                else:
                    with open(local_path, "r", encoding="utf-8") as f:
                        records_iterator = parser_func(f)

                tsv_stream = transformer_func(records_iterator)
                loader.bulk_load(staging_table, tsv_stream)
                logging.info(f"Finished loading data into {staging_table}.")

                # 3. Apply Changes (Full Load)
                if args.mode == "full":
                    loader.apply_changes(staging_table, prod_table, index_ddls)
                    loader.cleanup(staging_table, prod_table)

            logging.info("--- ETL process finished successfully. ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during the ETL process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
