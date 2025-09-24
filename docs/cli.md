# CLI Reference

The `py-load-medgen` tool is controlled via the command line.

## Installation

To use the tool, it is recommended to install it in a virtual environment. You can install it directly from the source code with the PostgreSQL driver:

```bash
pip install .[postgres]
```

To install for development, which includes testing and documentation dependencies:

```bash
pip install -e .[test,docs]
```

## Usage

The main command is `py-load-medgen`.

```bash
py-load-medgen [OPTIONS]
```

### Options

| Option                 | Type    | Default        | Environment Variable | Description                                                                                             |
| ---------------------- | ------- | -------------- | -------------------- | ------------------------------------------------------------------------------------------------------- |
| `--download-dir`       | `str`   | `.`            |                      | The directory to download the MedGen files to.                                                          |
| `--db-dsn`             | `str`   | `None`         | `MEDGEN_DB_DSN`      | The database connection string (DSN). Required. Can be set via this argument or the environment variable. |
| `--mode`               | `str`   | `full`         |                      | The load strategy: `full` for a complete refresh, or `delta` for incremental updates.                     |
| `--max-parse-errors`   | `int`   | `100`          |                      | The maximum number of parsing errors to tolerate before aborting the ETL process.                       |
| `-h`, `--help`         |         |                |                      | Show the help message and exit.                                                                         |

### Examples

**Example 1: Full Load**

Perform a full refresh of the database, downloading files to `/tmp/medgen_data`. The DSN is provided by an environment variable.

```bash
export MEDGEN_DB_DSN="postgresql://user:password@host:port/dbname"
py-load-medgen --mode full --download-dir /tmp/medgen_data
```

**Example 2: Delta Load with Low Error Tolerance**

Run an incremental update and abort if more than 10 parsing errors are found.

```bash
py-load-medgen --mode delta --max-parse-errors 10 --db-dsn "..."
```
