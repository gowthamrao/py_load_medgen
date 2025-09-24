# FRD-to-Implementation Mapping for `py-load-medgen`

This document provides a detailed mapping of the Functional Requirements Document (FRD) for `py-load-medgen` to the actual implementation in the codebase.

---

## 1. Introduction and Scope

This section of the FRD is high-level. The implementation of the overall scope is demonstrated by the existence and functionality of the package itself, driven by the CLI.

*   **Requirement:** FRD 1.2 - In Scope (PostgreSQL support, full/delta loads, CLI execution).
*   **Implementation:** The entire package, orchestrated by `src/py_load_medgen/cli.py`, which handles command-line arguments for mode (`--mode full/delta`) and database connection (`--db-dsn`).

---

## 2. Data Acquisition and Processing

### 2.1 Data Source and Download
*   **Requirement:** FRD 2.1 - Robust downloading from NCBI FTP, including retries and integrity checks.
*   **Implementation:** `src/py_load_medgen/downloader.py` - The `Downloader` class.
*   **Example (Retry Logic):** The `download_file` method uses the `tenacity` library for exponential backoff on errors.
    ```python
    # From: src/py_load_medgen/downloader.py

    from tenacity import retry, stop_after_attempt, wait_exponential

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def download_file(...):
        # ... download logic ...
    ```
*   **Example (File Integrity Check):** The `verify_file` method calculates the MD5 checksum of the downloaded file and compares it to the one from the server's `md5sum.txt`.
    ```python
    # From: src/py_load_medgen/downloader.py

    def verify_file(self, local_filepath: Path, checksums: dict[str, str]) -> bool:
        # ...
        expected_md5 = checksums[filename]
        actual_md5 = self._calculate_md5(local_filepath)
        if actual_md5 == expected_md5:
            return True
        # ...
    ```

### 2.3 Data Representation & 2.4 Metadata Management
*   **Requirement:** FRD 2.3.2 - Optional capture of the raw source record for auditability.
*   **Implementation:** The parsers and DDL schemas are designed to include a `raw_record` column.
*   **Example:** The staging table DDL for concepts includes this column, and the parser populates it.
    ```python
    # From: src/py_load_medgen/sql/ddl.py
    STAGING_CONCEPTS_DDL = """
    CREATE UNLOGGED TABLE IF NOT EXISTS staging_medgen_concepts (
        -- ... other columns
        raw_record TEXT
    );
    """

    # From: tests/test_integration.py (verifying the feature)
    def test_staging_load_with_raw_record(postgres_db_dsn):
        # ...
        cur.execute(f"SELECT COUNT(*), MIN(raw_record) FROM {STAGING_TABLE}")
        count, raw_record = cur.fetchone()
        assert count == 3
        assert raw_record == SAMPLE_MRCONSO_DATA.splitlines()[0]
    ```
*   **Requirement:** FRD 2.4 - Capture and store metadata about the ETL process.
*   **Implementation:** The `PostgresNativeLoader` has methods to write to an `etl_audit_log` table.
*   **Example:** `log_run_start` and `log_run_finish` methods in the loader.
    ```python
    # From: src/py_load_medgen/loader/postgres.py
    def log_run_start(...):
        sql = """
            INSERT INTO etl_audit_log (run_id, package_version, ...)
            VALUES (%s, %s, ...) RETURNING log_id;
        """
        # ...
        cur.execute(sql, ...)
        return cur.fetchone()[0]
    ```

---

## 3. Loading Strategies and Robustness

### 3.1 Full Load & 3.3 Idempotency and Atomicity
*   **Requirement:** FRD 3.1 & 3.3 - Atomic full loads using staging tables and a transactional swap.
*   **Implementation:** `src/py_load_medgen/loader/postgres.py` - The `_apply_full_load` method.
*   **Example (Atomic Swap):** The method renames tables within a single transaction block to ensure atomicity.
    ```python
    # From: src/py_load_medgen/loader/postgres.py
    def _apply_full_load(...):
        # ...
        with self.conn.transaction():
            cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
            cur.execute(f"ALTER TABLE IF EXISTS {production_table} RENAME TO {backup_table};")
            cur.execute(f"ALTER TABLE {new_production_table} RENAME TO {production_table};")
    ```

### 3.2 Delta Load (Incremental)
*   **Requirement:** FRD 3.2 - Mechanism for Change Data Capture (CDC) to identify new, updated, and deleted records.
*   **Implementation:** `src/py_load_medgen/loader/postgres.py` - The `execute_cdc` method.
*   **Example (CDC Logic):** The method uses `LEFT JOIN` operations between the staging and production tables to find differences.
    ```python
    # From: src/py_load_medgen/loader/postgres.py
    def execute_cdc(...):
        # ...
        # Find records in production that are NOT in the new staging data.
        sql_find_deletes = f"""
            INSERT INTO cdc_deletes (id)
            SELECT p.{pk_name}
            FROM {production_table} p
            LEFT JOIN {staging_table} s ON p.{business_key} = s.{business_key}
            WHERE s.{business_key} IS NULL AND p.is_active = true;
        """
        cur.execute(sql_find_deletes)
        # ...
    ```

---

## 4. Architecture and Extensibility

### 4.3 Database Abstraction Layer & 4.4 Extensibility
*   **Requirement:** FRD 4.3 & 4.4 - An abstract interface (`AbstractNativeLoader`) and a Factory/Strategy pattern to instantiate loaders.
*   **Implementation:** `src/py_load_medgen/loader/base.py` defines the ABC, and `src/py_load_medgen/loader/factory.py` implements the factory.
*   **Example (Abstract Base Class):**
    ```python
    # From: src/py_load_medgen/loader/base.py
    from abc import ABC, abstractmethod

    class AbstractNativeLoader(ABC):
        @abstractmethod
        def connect(self) -> None:
            raise NotImplementedError

        @abstractmethod
        def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
            raise NotImplementedError
        # ... other abstract methods
    ```
*   **Example (Loader Factory):**
    ```python
    # From: src/py_load_medgen/loader/factory.py
    from py_load_medgen.loader.base import AbstractNativeLoader
    from py_load_medgen.loader.postgres import PostgresNativeLoader

    class LoaderFactory:
        @staticmethod
        def create_loader(db_dsn: str) -> AbstractNativeLoader:
            parsed_uri = urlparse(db_dsn)
            scheme = parsed_uri.scheme
            if scheme in ("postgres", "postgresql"):
                return PostgresNativeLoader(db_dsn=db_dsn)
            else:
                raise ValueError(f"Unsupported database scheme: '{scheme}'.")
    ```

---

## 5. Database Implementation Details

### 5.2 PostgreSQL Implementation (Default)
*   **Requirement:** FRD 5.2.1 & 5.2.2 - Use of native `COPY` protocol and `UNLOGGED` tables for staging.
*   **Implementation:** `src/py_load_medgen/loader/postgres.py` and `src/py_load_medgen/sql/ddl.py`.
*   **Example (Native `COPY`):** The `bulk_load` method uses `psycopg`'s `copy()` context manager.
    ```python
    # From: src/py_load_medgen/loader/postgres.py
    def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
        # ...
        with self.conn.cursor() as cur:
            with cur.copy(f"COPY {table_name} FROM STDIN ...") as copy:
                for line in data_iterator:
                    copy.write(line)
    ```
*   **Example (`UNLOGGED` Staging Tables):** The DDL scripts for staging tables specify `UNLOGGED`.
    ```sql
    -- From: src/py_load_medgen/sql/ddl.py
    STAGING_CONCEPTS_DDL = """
    CREATE UNLOGGED TABLE IF NOT EXISTS staging_medgen_concepts (
        -- ... columns
    );
    """
    ```

---

## 6. Non-Functional Requirements

### 6.2 Configuration and Execution & 6.3 Observability
*   **Requirement:** FRD 6.2 - CLI tool with configuration via args and environment variables.
*   **Implementation:** `src/py_load_medgen/cli.py` uses the `argparse` library.
*   **Example:**
    ```python
    # From: src/py_load_medgen/cli.py
    parser.add_argument(
        "--db-dsn",
        type=str,
        default=os.environ.get("MEDGEN_DB_DSN"),
        help="PostgreSQL connection string (DSN). Can also be set via MEDGEN_DB_DSN...",
    )
    ```
*   **Requirement:** FRD 6.3 - Structured logging and error reporting.
*   **Implementation:** Standard `logging` is configured in `cli.py`, and the main `try...except` block logs failures to the database.
*   **Example:**
    ```python
    # From: src/py_load_medgen/cli.py
    except Exception as e:
        # ...
        if log_id is not None:
            with LoaderFactory.create_loader(...) as error_loader:
                error_loader.log_run_finish(
                    log_id,
                    status="Failed",
                    # ...
                    error_message=traceback.format_exc(),
                )
    ```

---

## 7. Software Engineering and Maintenance

### 7.1 Packaging & 7.2 Code Quality
*   **Requirement:** FRD 7.1 & 7.2 - Modern Python packaging (`pyproject.toml`) with linting and static analysis tools.
*   **Implementation:** The `pyproject.toml` file in the root directory.
*   **Example:**
    ```toml
    # From: pyproject.toml

    [project.optional-dependencies]
    postgres = ["psycopg[binary]"]
    test = ["pytest", "pytest-mock", "psycopg[binary]", "testcontainers[postgres]"]

    [project.scripts]
    py-load-medgen = "py_load_medgen.cli:main"

    [tool.ruff]
    line-length = 88

    [tool.mypy]
    mypy_path = "src"
    strict = true
    ```

### 7.3 Testing Strategy
*   **Requirement:** FRD 7.3 - Mandatory integration tests using Dockerized database instances.
*   **Implementation:** `tests/test_integration.py` uses `pytest` and `testcontainers`.
*   **Example:** The test dependencies in `pyproject.toml` and the use of the `postgres_db_dsn` fixture (provided by `testcontainers`) in the tests.
    ```python
    # From: tests/test_integration.py
    import pytest

    # The 'postgres_db_dsn' fixture is injected by the testcontainers plugin
    # configured in tests/conftest.py, which provides a connection string
    # to a temporary, live PostgreSQL database running in Docker.
    @pytest.mark.integration
    def test_full_load_atomic_swap_with_raw_record(postgres_db_dsn):
        with PostgresNativeLoader(db_dsn=postgres_db_dsn) as loader:
            # ... test logic that interacts with the live database ...
    ```
