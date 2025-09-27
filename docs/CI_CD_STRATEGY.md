# CI/CD Strategy

## 1. Overview

The goal of this CI/CD pipeline is to establish a robust, secure, and efficient automated workflow for the `py-load-medgen` project. This document outlines the architecture, tools, and rationale behind the implementation, ensuring clarity, efficiency, and security best practices are met.

The pipeline is designed to provide fast feedback to developers by separating linting and testing, and to ensure that the Docker container is secure and optimized for production.

## 2. Technology Stack and Rationale

The following tools have been selected to build the CI/CD pipeline:

-   **Dependency Management: Poetry**
    -   **Rationale:** The repository had an ambiguous dependency management setup, with configurations for both `setuptools` and `poetry`. Poetry was chosen as the definitive tool because it provides a unified and modern approach to dependency management, virtual environments, and packaging. The presence of a `poetry.lock` file and instructions in `AGENTS.md` indicated that Poetry was the intended tool. The `pyproject.toml` file has been consolidated to use Poetry exclusively.

-   **Linting and Formatting: pre-commit, Ruff, Black, Mypy**
    -   **Rationale:** To enforce code quality and consistency, a comprehensive `pre-commit` configuration has been added. This includes `Ruff` for fast linting, `Black` for standardized code formatting, and `Mypy` for static type checking. This ensures that all code contributions adhere to the project's quality standards before being merged.

-   **Testing: Pytest**
    -   **Rationale:** `pytest` is the established testing framework for this project, as indicated by the existing test suite and `pyproject.toml` configuration. The framework's support for markers (`unit` and `integration`) allows for a clear separation of test types, which is leveraged in the CI pipeline.

-   **Containerization: Docker**
    -   **Rationale:** A multi-stage `Dockerfile` has been introduced to create a lean, secure, and production-ready container. This approach minimizes the image size and attack surface by separating the build environment from the final runtime environment.

-   **Vulnerability Scanning: Trivy**
    -   **Rationale:** Trivy is integrated into the Docker workflow to scan the container image for known vulnerabilities. This provides a critical security gate, ensuring that only secure images are considered for deployment.

## 3. Proactive Improvements Made

Several key improvements were made to the repository to align with best practices:

-   **Consolidated Dependency Management:** The `pyproject.toml` file was refactored to use Poetry exclusively, removing the ambiguous `setuptools` configuration and creating a single source of truth for dependencies.
-   **Added Comprehensive `pre-commit` Configuration:** A `.pre-commit-config.yaml` file was created from scratch to automate code quality checks, including linting, formatting, and type checking.
-   **Introduced `Dockerfile` and `.dockerignore`:** A multi-stage `Dockerfile` was created to produce a lean and secure production image. A `.dockerignore` file was also added to minimize the Docker build context and improve build times.
-   **Resolved Ambiguous Project Setup:** By standardizing on Poetry and adding the necessary configuration files, the project now has a clear and modern structure that is easy to maintain.

## 4. Workflow Architecture

The CI/CD pipeline is split into two separate workflows: `ci.yml` and `docker.yml`.

### `ci.yml`

This workflow is responsible for linting and testing the codebase. It is designed for fast feedback and is triggered on every push and pull request to the `main` branch.

-   **Job 1: `lint`**
    -   This job runs first on an `ubuntu-latest` runner and uses the `pre-commit/action` to execute all configured hooks. This provides a quick check for any code quality issues.

-   **Job 2: `test`**
    -   This job depends on the successful completion of the `lint` job.
    -   It runs on a matrix of operating systems (`ubuntu-latest`, `macos-latest`, `windows-latest`) and Python versions (`3.11`, `3.12`) to ensure broad compatibility.
    -   It separates the execution of `unit` and `integration` tests, uploading coverage reports for each to Codecov.

### `docker.yml`

This workflow is responsible for building and scanning the Docker image. It is also triggered on every push and pull request to the `main` branch.

-   **Job: `build-and-scan`**
    -   This job runs on an `ubuntu-latest` runner.
    -   It logs into Docker Hub to prevent rate-limiting issues.
    -   It uses a multi-stage `Dockerfile` to build the image, with BuildKit caching enabled for faster builds.
    -   Finally, it uses Trivy to scan the built image for `HIGH` and `CRITICAL` vulnerabilities, failing the workflow if any are found.

## 5. Testing Strategy

The testing strategy is designed to be comprehensive and provide clear insights into code coverage.

-   **Matrix Testing:** Tests are run across multiple platforms and Python versions to ensure the code is portable and works as expected in different environments.
-   **Separation of Tests:** `unit` and `integration` tests are run as separate steps, allowing for easier debugging and more granular feedback.
-   **Code Coverage:** Coverage reports are generated for both unit and integration tests and uploaded to Codecov with unique flags for each combination of OS, Python version, and test type. This provides a detailed view of code coverage across the entire test matrix.

## 6. Dependency Management and Caching

-   **Poetry:** All Python dependencies are managed through Poetry and are defined in the `pyproject.toml` file.
-   **Installation:** In the CI workflow, Poetry is installed via `pip`, and then `poetry install` is used to install the project dependencies.
-   **Caching:** The `actions/setup-python` action is configured with `cache: 'poetry'` to automatically cache and restore dependencies, speeding up the CI process.

## 7. Security Hardening

The following security measures have been implemented in the CI/CD pipeline:

-   **Principle of Least Privilege (PoLP):** Workflows are configured with `permissions: contents: read` to ensure they only have the minimum required access.
-   **Action Pinning:** All third-party GitHub Actions are pinned to their full commit SHA to prevent the execution of malicious or unexpected code.
-   **Docker Hub Authentication:** The Docker workflow authenticates with Docker Hub to avoid rate-limiting and ensure reliable access to base images.
-   **Non-Root Docker User:** The `Dockerfile` creates a non-root user to run the application, reducing the potential impact of a container breakout.
-   **Vulnerability Scanning:** The Trivy action scans the Docker image for `HIGH` and `CRITICAL` vulnerabilities, failing the build if any are found.

## 8. Docker Strategy

-   **Multi-Stage Builds:** The `Dockerfile` uses a multi-stage build to create a lean production image. The builder stage installs all necessary build tools and dependencies, while the final stage copies only the application code and the required runtime dependencies.
-   **Caching:** The `docker.yml` workflow uses the GitHub Actions cache (`type=gha`) to cache Docker layers, which significantly speeds up subsequent builds.
-   **Verification and Scanning:** On every pull request, the Docker image is built and scanned for vulnerabilities, but not pushed. This ensures that all code changes are validated before being merged.

## 9. How to Run Locally

To replicate the CI checks locally, you can use the following commands:

-   **Run `pre-commit` checks:**
    ```bash
    pip install pre-commit
    pre-commit run --all-files
    ```

-   **Run tests:**
    ```bash
    pip install poetry
    poetry install --with test
    poetry run pytest
    ```

-   **Build the Docker image:**
    ```bash
    docker build -t py-load-medgen .
    ```