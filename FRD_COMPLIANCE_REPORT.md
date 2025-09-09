# FRD Compliance Report for `py_load_medgen`

This document provides a detailed, requirement-by-requirement analysis of how the `py_load_medgen` package meets the specifications outlined in the Functional Requirements Document (FRD).

---

## 2. Data Acquisition and Processing

### R-2.1.1 Connection Handling
**Requirement:** The downloader module must implement retry logic with exponential backoff.
**Status:** Implemented.
**Explanation:** The `download_file` method in the `Downloader` class is decorated with `@retry` from the `tenacity` library, which is configured for exponential backoff to handle transient network or FTP server issues.
**Example (`src/py_load_medgen/downloader.py`):**
```python
from tenacity import retry, stop_after_attempt, wait_exponential

# ...

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def download_file(
    self,
    # ...
) -> None:
    # ...
```

### R-2.1.2 Download Resumption
**Requirement:** The module SHOULD support resuming partial downloads.
**Status:** Implemented.
**Explanation:** The `download_file` method checks if a local file already exists and gets its size. It then uses the `rest` parameter of `ftplib.FTP.retrbinary` to instruct the FTP server to resume the download from that byte offset, and it opens the local file in append mode.
**Example (`src/py_load_medgen/downloader.py`):**
```python
# --- Resumption Logic ---
rest_pos = 0
open_mode = "wb"
if local_filepath.exists():
    rest_pos = local_filepath.stat().st_size
    open_mode = "ab"

# ...

with open(local_filepath, open_mode) as f:
    self.ftp.retrbinary(
        f"RETR {remote_filename}",
        f.write,
        rest=rest_pos if rest_pos > 0 else None,
    )
```

### R-2.1.3 File Integrity Verification
**Requirement:** After download, the integrity of the files MUST be verified using checksums.
**Status:** Implemented.
**Explanation:** The `Downloader` class has a `get_checksums` method to fetch the `md5sum.txt` file and a `verify_file` method that compares the expected MD5 hash with a locally calculated one. This is called automatically after a successful download.
**Example (`src/py_load_medgen/downloader.py`):**
```python
def verify_file(self, local_filepath: Path, checksums: dict[str, str]) -> bool:
    # ...
    expected_md5 = checksums[filename]
    actual_md5 = self._calculate_md5(local_filepath)
    if actual_md5 == expected_md5:
        return True
    # ...
```

### R-2.2.1 Format Support & R-2.2.4 Encoding
**Requirement:** The parser must support the required MedGen file formats, including structured delimited text files and UMLS RRF, and handle UTF-8 encoding.
**Status:** Implemented.
**Explanation:** The `src/py_load_medgen/parser.py` module contains multiple dedicated parsing functions (`parse_mrconso`, `parse_names`, `parse_mrrel`, etc.) that handle the pipe-delimited RRF format. It also uses `encoding="utf-8"` when opening gzipped files.
**Example (`src/py_load_medgen/parser.py`):**
```python
def parse_names(file_path: Path, max_errors: int) -> Iterator[MedgenName]:
    """Parses a gzipped, pipe-delimited NAMES.RRF.gz file."""
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        # ...
        for i, line in enumerate(f):
            row = raw_line.split("|")
            # ...
```

### R-2.2.3 Error Handling
**Requirement:** Parsing errors must be logged. A configurable threshold for an acceptable error rate shall be implemented.
**Status:** Implemented.
**Explanation:** All parsing functions in `parser.py` accept a `max_errors` parameter. The parser logs malformed rows and raises a `ValueError` if the error count exceeds this threshold.
**Example (`src/py_load_medgen/parser.py`):**
```python
def parse_mrconso(file_stream: IO[str], max_errors: int) -> Iterator[MrconsoRecord]:
    error_count = 0
    for i, line in enumerate(file_stream):
        # ...
        if len(row) < 19:
            error_count += 1
            logging.warning(f"Skipping malformed row {i+1}...")
            if error_count > max_errors:
                raise ValueError(f"Exceeded maximum parsing errors ({max_errors}).")
            continue
```

### R-2.3.2 Raw Data Capture
**Requirement:** The package SHALL provide an option to store the raw, unprocessed source record.
**Status:** Implemented.
**Explanation:** All parser dataclasses (e.g., `MrconsoRecord`, `MedgenName`) have a `raw_record` field. The DDL for all production tables also includes a `raw_record TEXT` column to store this data.
**Example (`src/py_load_medgen/sql/ddl.py`):**
```sql
PRODUCTION_CONCEPTS_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    -- ... other columns
    raw_record TEXT
);
"""
```

### R-2.4 Metadata Management
**Requirement:** A dedicated metadata schema/table must be maintained.
**Status:** Implemented.
**Explanation:** The `PostgresNativeLoader` contains methods (`log_run_start`, `log_run_finish`, `log_run_detail`) that write comprehensive execution metadata to the `etl_audit_log` and `etl_run_details` tables.
**Example (`src/py_load_medgen/loader/postgres.py`):**
```python
def log_run_start(
    self,
    run_id: uuid.UUID,
    package_version: str,
    # ...
) -> int:
    sql = (
        "INSERT INTO etl_audit_log (run_id, package_version, ...) "
        "VALUES (%s, %s, ...) RETURNING log_id;"
    )
    # ...
```

---

## 3. Loading Strategies and Robustness

### R-3.1.1 Full Load (Staging and Atomic Swap) & R-3.3 Atomicity
**Requirement:** Full loads must use staging tables and an atomic swap within a single transaction.
**Status:** Implemented.
**Explanation:** The `_apply_full_load` method in `PostgresNativeLoader` creates a new table, loads data into it, replicates indexes, and then performs the swap (`DROP`, `RENAME`, `RENAME`) inside a single `with self.conn.transaction():` block to ensure atomicity.
**Example (`src/py_load_medgen/loader/postgres.py`):**
```python
def _apply_full_load(...):
    # ...
    with self.conn.transaction():
        cur.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE;")
        cur.execute(f"ALTER TABLE IF EXISTS {production_table} RENAME TO {backup_table};")
        cur.execute(f"ALTER TABLE {new_production_table} RENAME TO {production_table};")
```

### R-3.2 Delta Load (Incremental)
**Requirement:** The system must identify new, updated, and deleted records for incremental loads.
**Status:** Implemented.
**Explanation:** The `execute_cdc` method in `PostgresNativeLoader` performs a comprehensive Change Data Capture by comparing the staging and production tables. It uses `LEFT JOIN`s to find inserts and deletes, and compares an MD5 hash of row data to find updates.
**Example (`src/py_load_medgen/loader/postgres.py`):**
```python
def execute_cdc(...):
    # ...
    # --- Find Deletes ---
    sql_find_deletes = f"INSERT INTO cdc_deletes (id) SELECT p.{pk_name} ..."
    cur.execute(sql_find_deletes)

    # --- Find Updates ---
    hash_comparison = f"MD5(ROW(...)) != MD5(ROW(...))"
    sql_find_updates = f"INSERT INTO cdc_updates SELECT s.* ... WHERE {hash_comparison};"
    cur.execute(sql_find_updates)

    # --- Find Inserts ---
    sql_find_inserts = f"INSERT INTO cdc_inserts SELECT s.* ..."
    cur.execute(sql_find_inserts)
    # ...
```

### R-3.2.5 Deletion Handling
**Requirement:** The package must identify records absent in the current source data and handle them appropriately (e.g., soft deletes).
**Status:** Implemented.
**Explanation:** The `_apply_delta_load` method performs a soft delete by updating a boolean `is_active` flag in the production table for all records identified for deletion by the CDC process.
**Example (`src/py_load_medgen/loader/postgres.py`):**
```python
# 2. Apply Deletes
sql_delete = (
    f"UPDATE {production_table} SET is_active = false, "
    f"last_updated_at = NOW() WHERE {pk_name} "
    f"IN (SELECT id FROM cdc_deletes);"
)
cur.execute(sql_delete)
```

---

## 4. Architecture and Extensibility

### R-4.3 Database Abstraction Layer & R-4.4 Extensibility
**Requirement:** An Abstract Base Class (`AbstractNativeLoader`) shall define the interface, and a Factory (`LoaderFactory`) shall instantiate the correct loader.
**Status:** Implemented.
**Explanation:** `src/py_load_medgen/loader/base.py` defines the `AbstractNativeLoader` ABC. `src/py_load_medgen/loader/factory.py` implements a `LoaderFactory` which returns a concrete loader instance based on the database connection string (DSN) scheme.
**Example (`src/py_load_medgen/loader/factory.py`):**
```python
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

### R-5.2.1 Native Bulk Loading (PostgreSQL)
**Requirement:** The `PostgresNativeLoader` MUST use the native PostgreSQL `COPY` protocol.
**Status:** Implemented.
**Explanation:** The `bulk_load` method uses `psycopg`'s `copy()` context manager, which is a highly efficient Python wrapper around the `COPY FROM STDIN` protocol, allowing for direct streaming from the parser to the database.
**Example (`src/py_load_medgen/loader/postgres.py`):**
```python
def bulk_load(self, table_name: str, data_iterator: Iterator[bytes]) -> None:
    with self.conn.cursor() as cur:
        with cur.copy(f"COPY {table_name} FROM STDIN ...") as copy:
            for line in data_iterator:
                copy.write(line)
```

### R-5.2.4 Staging Optimization
**Requirement:** Staging tables SHOULD be created as `UNLOGGED` tables.
**Status:** Implemented.
**Explanation:** The DDL strings for all staging tables in `src/py_load_medgen/sql/ddl.py` include the `UNLOGGED` keyword to improve write performance by bypassing the Write-Ahead Log.
**Example (`src/py_load_medgen/sql/ddl.py`):**
```sql
STAGING_CONCEPTS_DDL = """
CREATE UNLOGGED TABLE IF NOT EXISTS staging_medgen_concepts (
    -- ... columns
);
"""
```

---

## 6. Non-Functional Requirements

### NFR-6.2.1 CLI & NFR-6.2.2 Configuration Management
**Requirement:** The package must provide a clear CLI, with configuration managed via environment variables and/or arguments.
**Status:** Implemented.
**Explanation:** `src/py_load_medgen/cli.py` uses `argparse` to define the command-line interface. The `--db-dsn` argument shows a common pattern used in the application where it reads its default value from an environment variable (`MEDGEN_DB_DSN`).
**Example (`src/py_load_medgen/cli.py`):**
```python
parser.add_argument(
    "--db-dsn",
    type=str,
    default=os.environ.get("MEDGEN_DB_DSN"),
    help="PostgreSQL connection string (DSN). Can also be set via MEDGEN_DB_DSN...",
)
```

### NFR-7.3 Testing Strategy
**Requirement:** Mandatory integration tests are required for the Loader modules, utilizing actual, Dockerized instances of the target database.
**Status:** Implemented.
**Explanation:** The `tests/` directory contains a comprehensive test suite. `tests/test_integration.py` uses the `testcontainers` library, which programmatically starts and stops a live PostgreSQL database in a Docker container for each test run. This ensures the loader logic is tested against a real database environment.
**Example (`tests/test_integration.py`):**
```python
# The 'postgres_db_dsn' fixture is injected by the testcontainers plugin
# configured in tests/conftest.py, which provides a connection string
# to a temporary, live PostgreSQL database running in Docker.
@pytest.mark.integration
def test_full_load_atomic_swap_with_raw_record(postgres_db_dsn):
    with PostgresNativeLoader(db_dsn=postgres_db_dsn) as loader:
        # ... test logic that interacts with the live database ...
```
