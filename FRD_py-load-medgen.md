# Functional Requirements Document (FRD)
# `py-load-medgen`

| **Version** | **Date**       | **Author**                            | **Status** | **Comments**                               |
|-------------|----------------|---------------------------------------|------------|--------------------------------------------|
| 1.0         | 2025-09-08     | Senior Bioinformatics Solutions Architect | Draft      | Initial version based on project kickoff. |

---

## 1. Introduction and Scope

### 1.1 Purpose
The purpose of the `py-load-medgen` package is to provide a scalable, reliable, and high-performance Python-based solution for the Extraction, Transformation, and Loading (ETL) of the NCBI MedGen database into remote relational databases. The package will serve as a foundational component for downstream bioinformatics analysis, applications, and data warehousing by ensuring that a local or cloud-based database can be maintained as an accurate and up-to-date mirror of the public MedGen resource.

### 1.2 Scope
This document defines the functional requirements for the initial release of `py-load-medgen`.

**In Scope:**
*   **ETL Core Functionality:** Extraction from the NCBI MedGen FTP source, transformation into a normalized relational schema, and loading into a target database.
*   **Loading Strategies:** Support for both full (complete refresh) and delta (incremental) data loads.
*   **Default Database Support:** Out-of-the-box, optimized support for **PostgreSQL** (version 12+).
*   **Execution Environment:** The package will be executable as a Command Line Interface (CLI) tool.
*   **Deployment:** The package will be container-friendly and deployable via Docker.

**Out of Scope:**
*   **Initial Database Support:** Direct, native support for databases other than PostgreSQL (e.g., Redshift, Databricks, BigQuery) is out of scope for the initial release but will be enabled by the architectural design.
*   **Graphical User Interface (GUI):** No GUI will be developed for the package.
*   **Data Transformation Logic:** The package will not perform complex biological data integration or harmonization beyond what is necessary to represent the MedGen source data in a relational format.
*   **API/Service Hosting:** The package is an ETL tool, not a service or API for querying the loaded data.

### 1.3 Key Objectives
*   **Efficiency:** Utilize native database bulk loading mechanisms (e.g., PostgreSQL `COPY`) to achieve high-performance data ingestion, minimizing load times for large datasets.
*   **Extensibility:** Implement a modular, cloud-agnostic architecture with a database abstraction layer to facilitate the future addition of new database backends with minimal effort.
*   **Data Integrity:** Ensure data consistency and correctness through atomic transactions, robust error handling, and comprehensive metadata tracking.
*   **Maintainability:** Adhere to modern software engineering best practices, including rigorous testing, comprehensive documentation, and clean, typed code.

---

## 2. Data Acquisition and Processing

### 2.1 Data Source and Download
*   **FR-2.1.1:** The package MUST download data directly from the official NCBI MedGen FTP site (`ftp.ncbi.nlm.nih.gov/pub/medgen/`). The FTP URL shall be configurable.
*   **FR-2.1.2:** The download module MUST handle standard FTP connection errors (e.g., timeouts, authentication failures) gracefully and with configurable retry logic.
*   **FR-2.1.3:** The package MUST support download resumption for large files to recover from interrupted downloads.
*   **FR-2.1.4:** The package MUST verify the integrity of downloaded files, for example, by checking file sizes or using checksums if provided by the source.

### 2.2 Parsing
*   **FR-2.2.1:** The package MUST correctly parse the structured text files provided by MedGen (e.g., `names.txt`, `MedGen_HPO.txt`, `MGREL.txt`, `MGDEF.txt`).
*   **FR-2.2.2:** The parser MUST be designed to be resilient to minor, non-breaking format variations, such as changes in column order (if headers are present) or extra whitespace.
*   **FR-2.2.3:** The parser MUST efficiently stream data and avoid loading entire files into memory to support systems with limited resources.

### 2.3 Data Representation

#### 2.3.1 Standard Representation
*   **FR-2.3.1.1:** The transformation module MUST convert the parsed source data into a structured, normalized relational schema suitable for efficient querying. (See Section 5.1 for the proposed schema).

#### 2.3.2 Full Representation
*   **FR-2.3.2.1:** The package MUST provide an optional feature to store the original, unmodified source record for each transformed row.
*   **FR-2.3.2.2:** This raw record SHOULD be stored in a flexible format, such as a `JSONB` column in the corresponding table, to ensure auditability and data provenance.

### 2.4 Metadata Management
*   **FR-2.4.1:** The package MUST create and manage a dedicated metadata table in the target database.
*   **FR-2.4.2:** This table MUST store critical information about each ETL run, including:
    *   MedGen release version and/or download timestamp.
    *   ETL process start and end times.
    *   Load type (full or delta).
    *   Success or failure status.
    *   Record counts for each processed table/file.
    *   A unique identifier for the load batch.

---

## 3. Loading Strategies and Robustness

### 3.1 Full Load
*   **FR-3.1.1:** The full load process MUST completely replace all existing MedGen data in the target database with the new dataset.
*   **FR-3.1.2:** The default strategy for a full load MUST be to load data into staging tables, and upon successful completion, transactionally swap them with the production tables. This ensures zero downtime for readers of the production data. An alternative `TRUNCATE` and reload strategy may be offered as a configurable option.

### 3.2 Delta Load (Incremental)
*   **FR-3.2.1:** The package MUST be able to perform a delta load by identifying changes (new, updated, and deleted records) between the currently loaded MedGen version and a new one.
*   **FR-3.2.2:** Change Data Capture (CDC) will be implemented by downloading the new data files into a temporary location and comparing them against the data in the production tables or a snapshot of the previous source files.
*   **FR-3.2.3:** The delta load process MUST use the metadata table (FR-2.4.1) to identify the previously loaded version and determine the correct set of changes to apply.
*   **FR-3.2.4:** All `INSERT`, `UPDATE`, and `DELETE` operations for a delta load MUST be executed within a single transaction to ensure atomicity.

### 3.3 Idempotency and Atomicity
*   **FR-3.3.1:** All load operations MUST be idempotent. Re-running a failed load or re-running a completed load for the same MedGen version MUST NOT introduce data duplication or corruption.
*   **FR-3.3.2:** The loading process MUST use staging tables to prepare data before applying it to the production tables.
*   **FR-3.3.3:** The final step of applying changes (e.g., swapping staging and production tables via `ALTER TABLE ... RENAME`) MUST be atomic to prevent the database from being left in an inconsistent state.

---

## 4. Architecture and Extensibility

### 4.1 Cloud Agnosticism
*   **FR-4.1.1:** The core application logic MUST NOT have dependencies on cloud-provider-specific SDKs or services (e.g., AWS S3, Google Cloud Storage).
*   **FR-4.1.2:** The package MUST be packaged and distributable in a standard format (e.g., a Docker container) that can run in any environment.

### 4.2 Modularity
*   **FR-4.2.1:** The codebase MUST be organized into distinct, loosely-coupled modules with clear responsibilities, including:
    *   `downloader`: For acquiring data from the source.
    *   `parser`: For parsing source file formats.
    *   `transformer`: For mapping source data to the target schema.
    *   `loader`: For managing database interactions and loading data.
    *   `metadata`: For managing ETL run metadata.

### 4.3 Database Abstraction Layer
*   **FR-4.3.1:** An abstract base class (ABC), `AbstractNativeLoader`, MUST be defined to standardize the interface for all database-specific loading implementations.
*   **FR-4.3.2:** The `AbstractNativeLoader` interface MUST define methods for:
    *   `connect()`: Establishing a database connection.
    *   `prepare_staging()`: Creating temporary/staging tables.
    *   `bulk_load(table, data_stream)`: Executing a native bulk load operation.
    *   `apply_changes()`: Applying loaded data to production tables (e.g., swap, merge).
    *   `cleanup()`: Tearing down staging resources.
*   **FR-4.3.3:** The package MUST use a **Strategy** or **Factory** design pattern to select and instantiate the correct `AbstractNativeLoader` implementation at runtime based on the user's configuration (e.g., `db_type="postgresql"`).

### 4.4 Extensibility
*   **FR-4.4.1:** The package design MUST allow for the addition of new database backends by creating a new module that implements the `AbstractNativeLoader` interface.
*   **FR-4.4.2:** Each new database extension MUST encapsulate all database-specific logic, including its native bulk loading mechanism (e.g., Redshift `COPY` from an S3 staging location, which would be orchestrated by the extension).
*   **FR-4.4.3:** Documentation MUST be provided explaining how a developer can create and register a new database loader extension.

---

## 5. Database Implementation Details

### 5.1 Target Schema (Standard Representation)
*   **FR-5.1.1:** The package will create a normalized relational schema. A high-level proposal is as follows:
    *   **concepts** (`cui`, `preferred_name`, `definition`)
    *   **names** (`cui`, `name`, `type`, `source`)
    *   **sources** (`source_id`, `source_name`, `source_version`)
    *   **semantic_types** (`cui`, `sty`)
    *   **relationships** (`cui1`, `relationship`, `cui2`, `source`)
    *   **source_links** (`cui`, `source`, `source_id`)
*   **FR-5.1.2:** All tables will use standard SQL data types (`VARCHAR`, `TEXT`, `INTEGER`, `TIMESTAMP`) for maximum compatibility. `cui` (Concept Unique Identifier) will be the primary linking key.

### 5.2 PostgreSQL Implementation (Default)
*   **FR-5.2.1:** The PostgreSQL loader MUST use the native `COPY` protocol for all bulk data ingestion to ensure maximum performance. This should be implemented using a library like `psycopg` and its `copy()` methods.
*   **FR-5.2.2:** For staging, the loader SHOULD use `UNLOGGED` tables to reduce I/O overhead during the load process.
*   **FR-5.2.3:** The loader MUST efficiently stream data from the parser/transformer directly to the database via the `COPY` command, without writing intermediate files to disk unless absolutely necessary.
*   **FR-5.2.4:** Indexes and foreign key constraints on production tables MUST be managed appropriately. For full loads, they may be dropped on staging tables and re-created after the data is loaded and before the final swap, to speed up ingestion.

---

## 6. Non-Functional Requirements

### 6.1 Performance
*   **NFR-6.1.1:** The ETL process for a full MedGen load on a standard cloud database instance should be completed in a timeframe competitive with other industry-standard ETL tools. Specific benchmarks will be established during development.

### 6.2 Configuration and Execution
*   **NFR-6.2.1:** The package MUST be operable as a CLI tool (e.g., `py-load-medgen --db-uri postgresql://... --load-type full`).
*   **NFR-6.2.2:** All configuration parameters, including database credentials, FTP source, and load type, MUST be configurable via a combination of command-line arguments, environment variables, and an optional configuration file. Environment variables should take precedence to support containerized environments.

### 6.3 Observability
*   **NFR-6.3.1:** The package MUST implement structured logging (e.g., JSON format) to allow for easy parsing and analysis by log management systems.
*   **NFR-6.3.2:** Log levels (DEBUG, INFO, WARNING, ERROR) MUST be configurable.
*   **NFR-6.3.3:** The package MUST provide clear error messages and stack traces upon failure, and should report key performance metrics (e.g., rows/sec) during the load process.

---

## 7. Software Engineering and Maintenance

### 7.1 Packaging
*   **SE-7.1.1:** The package MUST use a modern Python packaging structure, including a `src` layout and a `pyproject.toml` file for configuration.
*   **SE-7.1.2:** Dependencies MUST be managed using Poetry.
*   **SE-7.1.3:** Optional dependencies for different database backends MUST be managed as package extras (e.g., `pip install py-load-medgen[postgres]`, `pip install py-load-medgen[redshift]`).

### 7.2 Code Quality
*   **SE-7.2.1:** The codebase MUST be formatted using `black`.
*   **SE-7.2.2:** The codebase MUST be linted using `ruff`.
*   **SE-7.2.3:** The codebase MUST use static type hints and pass static analysis with `mypy` in strict mode.
*   **SE-7.2.4:** A pre-commit hook configuration MUST be provided to automate these checks.

### 7.3 Testing Strategy
*   **SE-7.3.1:** A comprehensive suite of unit tests using `pytest` MUST be developed, covering all parsing, transformation, and business logic. Code coverage should be monitored and maintained at a high level (e.g., >90%).
*   **SE-7.3.2:** Integration tests are MANDATORY. These tests MUST validate the end-to-end ETL process against a live database instance.
*   **SE-7.3.3:** The integration testing framework MUST use Dockerized database instances (e.g., via `testcontainers-python`) to ensure a clean and reproducible testing environment, starting with PostgreSQL.

### 7.4 Documentation
*   **SE-7.4.1:** Comprehensive user and developer documentation MUST be created.
*   **SE-7.4.2:** The documentation MUST include:
    *   A high-level architecture diagram.
    *   A detailed data dictionary for the target database schema.
    *   Clear examples for configuration and execution.
    *   A developer guide explaining how to add support for a new database, including implementing the `AbstractNativeLoader` interface.
    *   API documentation for the core modules, automatically generated from docstrings.
