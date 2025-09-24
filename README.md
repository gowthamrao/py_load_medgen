# py-load-medgen

[![PyPI version](https://badge.fury.io/py/py-load-medgen.svg)](https://badge.fury.io/py/py-load-medgen)
[![Build Status](https://github.com/ohdsi/py-load-medgen/actions/workflows/ci.yml/badge.svg)](https://github.com/ohdsi/py-load-medgen/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ohdsi/py-load-medgen/branch/main/graph/badge.svg?token=YOUR_CODECOV_TOKEN)](https://codecov.io/gh/ohdsi/py-load-medgen)

A Python package for scalable, reliable, and high-performance ETL of the NCBI MedGen database.

## 📖 Table of Contents

- [Key Features](#-key-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Usage](#-usage)
- [Configuration](#-configuration)
- [Testing](#-testing)
- [Contributing](#-contributing)
- [License](#-license)

## ✨ Key Features

- **High-Performance ETL**: Optimized for speed using native database bulk loading protocols (e.g., PostgreSQL `COPY`).
- **Full & Delta Loads**: Supports both complete data refreshes and efficient incremental updates.
- **Extensible by Design**: Easily add support for new database systems (e.g., Redshift, BigQuery) through a clean abstraction layer.
- **Data Integrity**: Ensures data consistency and accuracy with transactional loads and file integrity checks.
- **Cloud-Agnostic**: Core logic is independent of any specific cloud provider.
- **Robust Error Handling**: Comprehensive logging and configurable error thresholds.
- **CLI Interface**: All operations are managed through a user-friendly Command Line Interface.

## 🏗️ Architecture

The `py-load-medgen` package is designed with a modular and extensible architecture that separates concerns into distinct layers:

1.  **Downloader**: Acquires data from the NCBI MedGen FTP site, handles retries, and verifies file integrity.
2.  **Parser/Transformer**: Interprets MedGen file formats, normalizes data into a relational schema, and handles data validation.
3.  **Loader**: Interacts with the target database. This layer is built around an `AbstractNativeLoader` interface, allowing for different database backends to be plugged in. The default implementation is `PostgresNativeLoader`.
4.  **Orchestrator**: Manages the end-to-end ETL workflow, including metadata logging and execution strategy (Full vs. Delta load).

This design is centered around a **Database Abstraction Layer**. To add support for a new database, you only need to implement the `AbstractNativeLoader` interface for that specific database, encapsulating all database-specific logic.

For a more detailed overview, see the [Architecture Documentation](./docs/architecture.md).

## 🚀 Installation

The package can be installed from PyPI. You need to specify the desired database driver as an "extra".

For PostgreSQL:

```bash
pip install "py-load-medgen[postgres]"
```

To install for development or to run tests:

```bash
git clone https://github.com/ohdsi/py-load-medgen.git
cd py-load-medgen
pip install -e ".[postgres,test,docs]"
```

## 💻 Usage

The primary interface for this package is the command-line tool `py_load_medgen`.

### Running an ETL Job

To run a full load into a PostgreSQL database:

```bash
py_load_medgen run \
    --mode full \
    --db-uri "postgresql://user:password@host:port/database"
```

To run a delta (incremental) load:

```bash
py_load_medgen run \
    --mode delta \
    --db-uri "postgresql://user:password@host:port/database"
```

For a full list of commands and options, use the `--help` flag:

```bash
py_load_medgen --help
py_load_medgen run --help
```

## ⚙️ Configuration

Configuration is managed through a combination of CLI arguments and environment variables, following the Twelve-Factor App methodology.

### Database Connection

The database connection string can be provided via the `--db-uri` argument or the `DB_URI` environment variable.

**Security Note**: For production environments, it is strongly recommended to set the database URI via the `DB_URI` environment variable rather than passing it as a CLI argument to avoid exposing secrets in shell history.

```bash
export DB_URI="postgresql://user:password@host:port/database"
py_load_medgen run --mode full
```

### Other Configurations

-   **`--log-level`**: Set the logging verbosity (e.g., `INFO`, `DEBUG`).
-   **`--error-threshold`**: The percentage of parsing errors to tolerate before failing the job.

## ✅ Testing

The project uses `pytest` for testing. The test suite includes both unit tests (mocking external services) and integration tests that run against a real, containerized PostgreSQL instance.

To run all tests:

```bash
pytest
```

To run only unit tests:

```bash
pytest -m unit
```

To run only integration tests (requires Docker):

```bash
pytest -m integration
```

Code quality is enforced with `ruff` (linting/formatting) and `mypy` (static type checking).

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Develop your changes and add corresponding tests.
4.  Ensure all tests and quality checks pass.
5.  Submit a pull request with a clear description of your changes.

For more details, please see the [Extending the Loader](./docs/extending.md) guide if you plan to add support for a new database.

## License

This project is **Source-Available** and dual-licensed.

The software is available under the [Prosperity Public License 3.0.0](LICENSE). You may use the software for non-commercial purposes, or for a commercial trial period of up to 30 days.

Commercial use beyond the 30-day trial period requires a separate commercial license. Please contact Gowtham Adamane Rao for details.
