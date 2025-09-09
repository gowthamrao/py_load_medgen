# Extending `py-load-medgen` with New Database Loaders

One of the core architectural goals of `py-load-medgen` is extensibility. The package is designed to support various relational database systems beyond the default PostgreSQL implementation. This guide details the process for developers to contribute new database loader modules.

## Core Concepts

The database loading mechanism is built on two key components that enforce a standardized interface while allowing for implementation-specific logic:

1.  **`AbstractNativeLoader` (The Interface):** An Abstract Base Class (ABC) that defines the contract for all loader classes. Any new loader you create **must** inherit from this class and implement all its abstract methods. This ensures that the orchestrator can interact with any database loader in a consistent way. The interface is located at `src/py_load_medgen/loader/base.py`.

2.  **`LoaderFactory` (The Factory):** A factory class responsible for instantiating the correct concrete loader based on the database connection string (DSN) provided at runtime. This decouples the client code from the specific loader implementations. The factory is located at `src/py_load_medgen/loader/factory.py`.

## Steps to Add a New Loader

Let's walk through the process of adding a hypothetical loader for **Amazon Redshift**.

### Step 1: Create the New Loader Module

Create a new Python file in the `src/py_load_medgen/loader/` directory. The name should be descriptive, for example, `redshift.py`.

### Step 2: Implement the `AbstractNativeLoader` Interface

In your new `redshift.py` file, create a class that inherits from `AbstractNativeLoader` and implements its methods.

```python
# In: src/py_load_medgen/loader/redshift.py
import logging
from typing import Iterator, Optional

# boto3 would be a new dependency for this loader
import boto3
import psycopg # Redshift uses the PG wire protocol

from py_load_medgen.loader.base import AbstractNativeLoader

class RedshiftNativeLoader(AbstractNativeLoader):
    """
    A native loader for Amazon Redshift.

    This loader leverages Redshift's native `COPY` command by first staging
    the data in an S3 bucket and then initiating the load.
    """

    def __init__(self, db_dsn: str, s3_bucket: str, aws_region: str):
        self.dsn = db_dsn
        self.conn: Optional[psycopg.Connection] = None

        # Redshift-specific configuration
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.s3_client = boto3.client("s3", region_name=self.aws_region)

    def connect(self) -> None:
        """Establishes a connection to the Redshift cluster."""
        logging.info("Connecting to Amazon Redshift...")
        self.conn = psycopg.connect(self.dsn)
        logging.info("Connection successful.")

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
        """
        Stages data to S3 and uses Redshift's COPY command.
        """
        if not self.conn:
            raise ConnectionError("Database connection not established.")

        # 1. Stage data to S3 (implementation-specific logic)
        s3_key = f"medgen-etl/{table_name}/{uuid.uuid4()}.txt.gz"
        logging.info(f"Uploading data to s3://{self.s3_bucket}/{s3_key}")

        # In a real implementation, you would stream this upload
        # For brevity, we'll concatenate and upload.
        gzipped_data = self._compress_iterator(data_iterator)
        self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=gzipped_data)

        # 2. Execute the native Redshift COPY command
        # Note: You need to provide AWS credentials for the COPY command.
        # This is best handled via IAM roles attached to the Redshift cluster.
        sql = f"""
            COPY {table_name}
            FROM 's3://{self.s3_bucket}/{s3_key}'
            IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftCopyRole'
            FORMAT AS TEXT
            GZIP;
        """
        logging.info(f"Executing Redshift COPY for table {table_name}")
        with self.conn.cursor() as cur:
            cur.execute(sql)

        logging.info("Redshift COPY command complete.")

    # You would need to implement all other abstract methods:
    # - initialize_staging()
    # - apply_changes()
    # - execute_cdc()
    # - cleanup()
    # - log_run_start(), log_run_finish(), etc.

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _compress_iterator(self, data_iterator: Iterator[bytes]) -> bytes:
        # Helper method to gzip the data before uploading
        import gzip
        return gzip.compress(b"".join(data_iterator))

```

**Key Considerations for Implementation:**

*   **Native Loading:** The primary goal is to use the database's most efficient bulk loading mechanism. For Redshift, this is the `COPY` command from S3. For other databases, it might be different (e.g., Databricks `COPY INTO`).
*   **Dependencies:** Your new loader might require additional Python packages (like `boto3` for AWS). These should be added as an optional dependency in `pyproject.toml`.
*   **Configuration:** Database-specific parameters (like the `s3_bucket` for Redshift) should be passed during initialization. This will require updates to the CLI and configuration handling.

### Step 3: Update the `LoaderFactory`

Modify `src/py_load_medgen/loader/factory.py` to recognize the DSN for your new database and instantiate your new loader.

```python
# In: src/py_load_medgen/loader/factory.py
from urllib.parse import urlparse

from py_load_medgen.loader.base import AbstractNativeLoader
from py_load_medgen.loader.postgres import PostgresNativeLoader
# Import your new loader
from py_load_medgen.loader.redshift import RedshiftNativeLoader

class LoaderFactory:
    @staticmethod
    def create_loader(db_dsn: str, config: dict) -> AbstractNativeLoader:
        """
        Creates a database loader instance based on the DSN scheme.

        Args:
            db_dsn: The database connection string.
            config: A dictionary of additional configuration options.
        """
        parsed_uri = urlparse(db_dsn)
        scheme = parsed_uri.scheme

        if scheme in ("postgres", "postgresql"):
            return PostgresNativeLoader(db_dsn=db_dsn)

        # Add the new scheme for Redshift
        elif scheme in ("redshift", "redshift+psycopg"):
            # Extract Redshift-specific config passed from the CLI
            s3_bucket = config.get("s3_bucket")
            aws_region = config.get("aws_region")
            if not s3_bucket or not aws_region:
                raise ValueError("Redshift loader requires 's3_bucket' and 'aws_region' config.")
            return RedshiftNativeLoader(db_dsn=db_dsn, s3_bucket=s3_bucket, aws_region=aws_region)

        else:
            raise ValueError(f"Unsupported database scheme: '{scheme}'.")
```

### Step 4: Add Optional Dependencies

In `pyproject.toml`, add the new dependencies under a new optional dependency group.

```toml
# In: pyproject.toml

[project.optional-dependencies]
postgres = ["psycopg[binary]"]
# Add a new group for redshift
redshift = ["psycopg[binary]", "boto3"]
test = ["pytest", "pytest-mock", "psycopg[binary]", "testcontainers[postgres]"]
docs = ["mkdocs", "mkdocs-material", "mkdocstrings[python]"]
```

### Step 5: Write Integration Tests

Following the pattern in `tests/test_integration.py`, create a new test file for your loader. While `testcontainers` may not support all databases, you can use mocking (`pytest-mock`) to test the logic of your loader class, such as verifying that the correct S3 upload and `COPY` commands are generated.

This structured approach ensures that `py-load-medgen` remains a robust, maintainable, and extensible tool for ETL.
