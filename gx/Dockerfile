# Use a standard Python image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install system dependencies for PostgreSQL
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create GX directory structure
RUN mkdir -p /app/great_expectations/uncommitted/data_docs \
    && mkdir -p /app/great_expectations/expectations \
    && mkdir -p /app/great_expectations/checkpoints \
    && mkdir -p /app/scripts

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose ports for Jupyter and Data Docs
EXPOSE 8888 8080

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
