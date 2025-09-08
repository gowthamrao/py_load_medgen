# py-load-medgen

**A Python package for scalable, reliable, and high-performance ETL of the NCBI MedGen database.**

`py-load-medgen` is a command-line tool designed to download, parse, and load the entire NCBI MedGen dataset into a relational database, with initial support optimized for PostgreSQL. It is built to be extensible, allowing for future integrations with other database systems.

## Key Features

- **High-Performance Loading:** Utilizes native database bulk loading protocols (e.g., PostgreSQL `COPY`) for maximum efficiency.
- **Robust Downloading:** Includes automatic retries with exponential backoff and download resumption for handling unstable network connections.
- **Data Integrity:** Verifies file integrity using MD5 checksums and ensures transactional, all-or-nothing data loads.
- **Full and Delta Modes:** Supports both complete refreshes of the dataset and incremental updates.
- **Extensible Architecture:** A clean, modular design allows for new database loaders to be added with minimal effort.
- **Comprehensive Logging:** Captures detailed metadata about each ETL run for auditability and monitoring.

## Getting Started

To get started, see the [CLI Reference](cli.md) for installation and usage instructions.
