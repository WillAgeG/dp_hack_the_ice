# Use the official Python image
FROM python:3.12-slim

# Define build arguments with the latest versions
ARG POETRY_VERSION=1.8.4
ARG PYSPARK_VERSION=3.5.3
ARG JAVA_VERSION=17

# Set the working directory
WORKDIR /opt/app

# Environment variables
ENV VIRTUAL_ENV=/opt/app/venv \
    PATH="/opt/app/venv/bin:${PATH}" \
    PYTHONPATH=/opt/app/prediction \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false

# Install necessary system packages, including Java 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        build-essential \
        openjdk-17-jdk-headless \
        procps && \
    rm -rf /var/lib/apt/lists/*

# Dynamically set JAVA_HOME and create a symlink to /usr/lib/jvm/default-java
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME is $JAVA_HOME" && \
    ln -s "$JAVA_HOME" /usr/lib/jvm/default-java

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --version "$POETRY_VERSION" && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy dependency files
COPY ./pyproject.toml ./poetry.lock /opt/app/

# Install Python dependencies with Poetry
RUN poetry install --no-interaction --no-root --without dev

# Install specific version of PySpark
RUN pip install --no-cache-dir pyspark=="$PYSPARK_VERSION"

# Copy the application source code and tests
COPY ./prediction /opt/app/prediction
COPY ./tests/spark /opt/app/tests/spark

# Create a non-root user for security
RUN useradd -m appuser

# Set permissions for the working directory
RUN chown -R appuser:appuser /opt/app

# Switch to the non-root user
USER appuser

# Define the entrypoint
# ENTRYPOINT [ "python3", "prediction/main.py" ]
