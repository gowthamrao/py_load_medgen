## Agent Instructions

### Running Tests

When running tests in this repository, you may encounter permission errors or timeouts when interacting with the Docker daemon. The following steps will guide you through the correct procedure for setting up the environment and running the tests.

**1. Install Dependencies:**

The project uses `poetry` for dependency management. Some dependencies, including `pytest`, are defined as optional dependencies. You must install these optional dependencies before running the tests.

Use the following command to install all necessary dependencies:

```bash
poetry install -E test -E docs
```

**2. Run Tests with `sudo`:**

The integration tests require access to the Docker daemon. In this environment, the most reliable way to achieve this is to run the tests with `sudo`.

However, running `sudo poetry` directly will not work due to `PATH` issues. You must use the full path to the `poetry` executable. You can find this path with `which poetry`. In the standard environment for this repo, the path is `/home/jules/.local/bin/poetry`.

Use the following command to run the full test suite:

```bash
sudo /home/jules/.local/bin/poetry run pytest
```

**Summary of Commands:**

```bash
# Install all dependencies, including optional test and docs dependencies
poetry install -E test -E docs

# Run the tests using sudo and the full path to poetry
sudo /home/jules/.local/bin/poetry run pytest
```

**Note:** The original `run-tests.sh` script, which uses `sg docker`, may not work in all environments and can lead to timeouts. The method described above is the recommended way to run the tests.
