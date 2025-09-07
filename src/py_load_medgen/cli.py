import argparse
import logging
import os
import sys

from py_load_medgen.loader.postgres import PostgresNativeLoader
from py_load_medgen.parser import parse_mrconso, records_to_tsv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    """Main CLI entry point for the MedGen ETL tool."""
    parser = argparse.ArgumentParser(description="A CLI tool for loading NCBI MedGen data into a database.")
    parser.add_argument(
        "--file-path",
        type=str,
        required=True,
        help="The full path to the local MRCONSO.RRF file to process.",
    )
    parser.add_argument(
        "--db-dsn",
        type=str,
        default=os.environ.get("MEDGEN_DB_DSN"),
        help="PostgreSQL connection string (DSN). "
             "Can also be set via the MEDGEN_DB_DSN environment variable.",
    )

    args = parser.parse_args()

    if not args.db_dsn:
        logging.error("Database connection string is required. Please provide it via the --db-dsn argument or the MEDGEN_DB_DSN environment variable.")
        sys.exit(1)

    staging_table = "staging_medgen_concepts"

    try:
        # Instantiate the loader for the target database
        loader = PostgresNativeLoader(db_dsn=args.db_dsn)

        # Use the loader as a context manager to handle connections
        with loader:
            # 1. Initialize the staging environment (create/truncate tables)
            logging.info("Initializing staging environment...")
            loader.initialize_staging()
            logging.info("Staging environment initialized.")

            # 2. Open the source file, parse it, and convert to a data stream
            logging.info(f"Opening source file: {args.file_path}")
            with open(args.file_path, "r", encoding="utf-8") as f:
                # The parser yields records one by one (memory efficient)
                records_iterator = parse_mrconso(f)

                # The TSV converter creates an in-memory stream for the COPY command
                tsv_stream = records_to_tsv(records_iterator)

                # 3. Perform the high-performance bulk load
                logging.info(f"Loading data into staging table: {staging_table}")
                loader.bulk_load(staging_table, tsv_stream)
                logging.info("Data loading complete.")

        logging.info("ETL process finished successfully.")

    except FileNotFoundError:
        logging.error(f"Error: The file was not found at {args.file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
