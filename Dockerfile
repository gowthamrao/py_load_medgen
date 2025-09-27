# ---- Builder Stage ----
# This stage installs dependencies using Poetry
FROM python:3.11-slim AS builder

# Set environment variables for Poetry
ENV POETRY_VERSION=1.8.2
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="$POETRY_HOME/bin:$PATH"

# Install Poetry
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Copy project definition files
WORKDIR /app
COPY pyproject.toml poetry.lock ./

# Install only production dependencies
RUN poetry install --no-dev --no-interaction --no-ansi

# ---- Final Stage ----
# This stage creates the final, lean production image
FROM python:3.11-slim AS final

# Create a non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -m appuser

# Copy installed dependencies from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application source code
COPY src/ /app/src

# Set environment to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app

# Switch to the non-root user
USER appuser

# Set the entrypoint for the application's CLI
ENTRYPOINT ["python", "-m", "py_load_medgen.cli"]
CMD ["--help"]