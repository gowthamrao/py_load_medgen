# Architecture

The `py-load-medgen` package is designed with a modular, decoupled architecture to promote maintainability, testability, and extensibility. The core logic is separated into four main components, managed by a central orchestrator.

## Core Components

![Architecture Diagram](https://placeholder.com/image.png)  <!-- Placeholder for a real diagram -->

1.  **Orchestrator (`cli.py`)**
    *   **Responsibility:** Manages the end-to-end ETL workflow.
    *   **Details:** The Command-Line Interface (CLI) acts as the entry point. It parses user arguments, coordinates the other components, and ensures the ETL process runs in the correct sequence (Download -> Parse -> Load). It is also responsible for high-level error handling and logging the final status of the run.

2.  **Downloader (`downloader.py`)**
    *   **Responsibility:** Acquiring and verifying data from the NCBI FTP server.
    *   **Details:** This module handles all FTP interactions. It is built to be robust, with features like connection retries, download resumption, and MD5 checksum validation to ensure the integrity of the source data.

3.  **Parser / Transformer (`parser.py`)**
    *   **Responsibility:** Interpreting source files and transforming them into a structured format.
    *   **Details:** This component contains parsers for each MedGen file format (e.g., `MRCONSO.RRF`, `NAMES.RRF`). It reads the raw, delimited text files and transforms each row into a structured Python `dataclass`. This module is designed to be a streaming iterator, which keeps memory usage low even for very large files. It also captures the raw source line for provenance.

4.  **Loader (`loader/`)**
    *   **Responsibility:** Loading the transformed data into the target database.
    *   **Details:** This is the most extensible part of the application. It uses a Factory pattern (`LoaderFactory`) and an Abstract Base Class (`AbstractNativeLoader`) to decouple the core logic from any specific database implementation. The default `PostgresNativeLoader` uses the highly efficient `COPY` command to bulk-load data from the parser's stream directly into staging tables. The loader is also responsible for managing database transactions to ensure atomicity.

## Extensibility

To add support for a new database (e.g., Redshift or BigQuery), a developer only needs to:
1.  Create a new loader class that inherits from `AbstractNativeLoader`.
2.  Implement the required methods (`initialize_staging`, `bulk_load`, `apply_changes`, etc.), making sure to use the target database's native bulk-loading mechanism.
3.  Register the new loader in the `LoaderFactory`.

This design ensures that database-specific logic is encapsulated within its own module, keeping the rest of the codebase agnostic.
