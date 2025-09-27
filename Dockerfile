# ---- Builder Stage ----
# Use a specific Python version for reproducibility
FROM python:3.12-slim as builder

# Set working directory
WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy only the files needed for dependency installation
COPY pyproject.toml poetry.lock ./

# Install dependencies, including optional ones for production
# --no-root is important to prevent poetry from creating a venv
RUN poetry install --no-root --no-dev --with postgres

# Export dependencies to a requirements.txt file
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes


# ---- Final Stage ----
# Use a specific, lean Python version
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Create a non-root user and group
RUN addgroup --system app && adduser --system --group app

# Copy requirements and install them
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code
COPY src/ /app/src/

# Set the entrypoint for the application
ENTRYPOINT ["python", "-m", "src.py_load_medgen.cli"]

# Switch to the non-root user
USER app