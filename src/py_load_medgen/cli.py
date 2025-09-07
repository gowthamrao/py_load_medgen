import argparse
import logging
import os
import sys
from pathlib import Path

from py_load_medgen.downloader import Downloader
from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, records_to_tsv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
NCBI_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
NCBI_FTP_PATH = "/pub/medgen/"
MRCONSO_REMOTE_FILE = "MRCONSO.RRF"


def main():
    """Main CLI entry point for the MedGen ETL tool."""
    parser = argparse.ArgumentParser(description="A CLI tool for loading NCBI MedGen data into a database.")
    parser.add_argument(
        "--file-path",
        type=str,
        help="The full path to the local MRCONSO.RRF file. If not provided, the file will be downloaded.",
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
        logging.error("Database connection string is required. Please provide it via --db-dsn or MEDGEN_DB_DSN.")
        sys.exit(1)

    file_path = args.file_path
    if not file_path:
        download_path = Path(args.download_dir) / MRCONSO_REMOTE_FILE
        logging.info(f"File path not provided. Attempting to download to {download_path}")
        try:
            with Downloader(ftp_host=NCBI_FTP_HOST, ftp_path=NCBI_FTP_PATH) as downloader:
                downloader.download_file(MRCONSO_REMOTE_FILE, download_path)
            file_path = download_path
        except Exception as e:
            logging.error(f"Failed to download file: {e}")
            sys.exit(1)

    try:
        loader = PostgresNativeLoader(db_dsn=args.db_dsn)

        with loader:
            logging.info("Initializing staging environment...")
            loader.initialize_staging()
            logging.info("Staging environment initialized.")

            logging.info(f"Opening source file: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                records_iterator = parse_mrconso(f)
                tsv_stream = records_to_tsv(records_iterator)

                logging.info(f"Loading data into staging table: {loader.staging_table}")
                loader.bulk_load(loader.staging_table, tsv_stream)
                logging.info("Data loading complete.")

            if args.mode == "full":
                logging.info("Starting full load process...")
                loader.apply_changes()
                loader.cleanup()
                logging.info("Full load process finished successfully.")

    except FileNotFoundError:
        logging.error(f"Error: The file was not found at {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
