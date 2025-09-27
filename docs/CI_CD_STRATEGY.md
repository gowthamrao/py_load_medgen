# CI/CD Strategy and Architecture

## 1. Overview

The goal of this CI/CD pipeline is to establish a modern, secure, and efficient automated workflow for the `py-load-medgen` repository. This pipeline ensures that every code change is automatically built, tested, and scanned, enforcing code quality, preventing regressions, and enhancing security.

The strategy is built on the following core principles:
- **Proactive Improvement:** Identifying and filling gaps in the repository's tooling and infrastructure.
- **Security:** Implementing best practices such as the Principle of Least Privilege (PoLP), action pinning, and vulnerability scanning.
- **Efficiency:** Using caching mechanisms for dependencies and Docker layers to ensure fast feedback loops.
- **Reliability:** Employing a matrix testing strategy to validate changes across multiple Python versions and operating systems.
- **Clarity:** Providing clear, actionable feedback to developers and maintaining comprehensive documentation.

## 2. Technology Stack and Rationale

The CI/CD pipeline leverages the following tools:

- **Dependency Management:** **Poetry** is used for managing Python dependencies and packaging. It provides deterministic builds via the `poetry.lock` file, which is crucial for reliable CI.
- **Testing Framework:** **Pytest** is used for running both unit and integration tests. Its powerful features, fixture system, and plugin ecosystem (including `pytest-cov` for coverage) make it ideal for this project.
- **Linting & Formatting:** **Ruff** and **Mypy** are used for static analysis. Ruff provides extremely fast linting and formatting, while Mypy ensures strict type checking.
- **Automation:** **GitHub Actions** is the core engine for the CI/CD workflows, chosen for its deep integration with the source code repository.
- **Containerization:** **Docker** is used to create portable, isolated environments for the application.
- **Security Scanning:** **Trivy** is used to scan the Docker image for known vulnerabilities in OS packages and application dependencies.
- **Code Coverage:** **Codecov** is used for tracking test coverage over time and providing detailed reports within pull requests.

## 3. Proactive Improvements Made

As part of this initiative, several key files were created to modernize the repository and establish best practices:

- **`.pre-commit-config.yaml`**: This file was added to integrate automated linting and formatting checks before code is committed. It uses `ruff` and `mypy` to enforce a consistent code style and catch type errors early, reducing the burden on developers and CI runners.

- **`Dockerfile`**: A multi-stage `Dockerfile` was introduced to create lean, secure, and production-ready container images.
  - The **builder stage** installs dependencies using Poetry.
  - The **final stage** copies only the necessary application code and the virtual environment from the builder. It creates a non-root user (`appuser`) to run the application, adhering to the security principle of least privilege and reducing the container's attack surface.

## 4. Workflow Architecture

The CI/CD pipeline is composed of two distinct GitHub Actions workflows:

### `ci.yml` (Core CI)
- **Triggers:** Runs on `push` and `pull_request` events targeting the `main` branch.
- **Jobs:**
  - **`lint`**: A fast-running job that uses `pre-commit/action` to run all configured linting and formatting checks. This serves as a quick quality gate.
  - **`test`**: A comprehensive job that runs after `lint` succeeds. It uses a test matrix to validate the code across:
    - **Python Versions:** `3.11`, `3.12`
    - **Operating Systems:** `ubuntu-latest`, `macos-latest`, `windows-latest`

### `docker.yml` (Docker Build & Scan)
- **Triggers:** Runs on `push` and `pull_request` events to `main`, but only when relevant files (`Dockerfile`, source code, or dependency files) are changed. This prevents unnecessary runs.
- **Jobs:**
  - **`build-and-scan`**: Builds the Docker image defined in the `Dockerfile`. It does not push the image but loads it into the runner to make it available for scanning. It then uses `aquasecurity/trivy-action` to scan the image for vulnerabilities.

## 5. Testing Strategy

- **Separation of Concerns:** Tests are split into `unit` and `integration` directories.
  - **Unit Tests:** Run on all jobs in the test matrix. They are fast and do not have external dependencies.
  - **Integration Tests:** Run only on `ubuntu-latest`. These tests use `testcontainers` to spin up a real PostgreSQL database in Docker, and therefore require a Docker environment.
- **Code Coverage:**
  - Separate coverage reports (`coverage-unit.xml` and `coverage-integration.xml`) are generated for each test suite.
  - These reports are uploaded to **Codecov** with distinct flags (e.g., `ubuntu-latest-py3.12-unit`) that uniquely identify the environment and test type. This allows for granular analysis of test coverage in Codecov's UI.

## 6. Code Quality and Linting

- **`pre-commit`**: Code quality is enforced primarily through `pre-commit` hooks defined in `.pre-commit-config.yaml`.
- **CI Enforcement**: The `lint` job in the `ci.yml` workflow runs these same hooks, ensuring that no code that violates the defined standards can be merged into `main`.

## 7. Dependency Management and Caching

- **Poetry Caching:** The `ci.yml` workflow uses the built-in caching mechanism of the `actions/setup-python` action (`cache: 'poetry'`). This significantly speeds up dependency installation by caching the Poetry virtual environment based on a hash of the `poetry.lock` file.
- **Docker Layer Caching:** The `docker.yml` workflow uses the GitHub Actions cache backend for BuildKit (`cache-from: type=gha` and `cache-to: type=gha,mode=max`). This caches individual Docker layers, dramatically speeding up subsequent image builds.

## 8. Security Hardening

Several measures have been implemented to secure the CI/CD pipeline:

- **Action Pinning:** All third-party GitHub Actions in both workflows are pinned to their full-length commit SHA. This prevents supply chain attacks where a malicious actor could take over a mutable tag (e.g., `v4`).
- **Principle of Least Privilege (PoLP):** Workflows are configured with `permissions: contents: read` by default, granting only the minimum permissions required.
- **Non-Root Docker User:** The `Dockerfile` creates and switches to a dedicated `appuser` before running the application, preventing processes from running with root privileges inside the container.
- **Vulnerability Scanning:** The `docker.yml` workflow scans every newly built image with **Trivy** for `HIGH` and `CRITICAL` vulnerabilities and will fail the build if any are found.

## 9. Docker Strategy

- **Multi-Stage Builds:** The `Dockerfile` uses a multi-stage build to separate the build environment from the final runtime environment. This ensures the final image is as small as possible and does not contain unnecessary build tools like Poetry itself.
- **Build on Change:** The `docker.yml` workflow is configured to only run when the application source code or Docker-related files change, conserving CI resources.
- **Verification, Not Deployment:** The workflow focuses on building and scanning the image as a verification step. Pushing the image to a registry is not included but could be added as a subsequent step for release workflows.

## 10. How to Run Locally

Developers can replicate the CI checks locally to ensure their changes will pass before pushing.

### Running Linters
1.  Install `pre-commit`: `pip install pre-commit`
2.  Set up the git hooks: `pre-commit install`
3.  Run all checks: `pre-commit run --all-files`

### Running Tests
1.  Install dependencies: `poetry install -E test -E docs`
2.  Run unit tests: `poetry run pytest tests/unit`
3.  Run integration tests (requires Docker): `poetry run pytest tests/integration`

### Building the Docker Image
1.  Ensure the Docker daemon is running.
2.  From the repository root, run: `docker build -t py-load-medgen:local .`
3.  Scan the local image with Trivy (requires Trivy installation): `trivy image py-load-medgen:local`