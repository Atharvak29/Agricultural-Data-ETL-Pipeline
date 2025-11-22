# Use a base image with Python and necessary build tools
FROM python:3.10-slim

# Set environment variables for Spark and Java
# Setting these inside the container ensures the PySpark process can find its dependencies.
ENV SPARK_HOME="/usr/local/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON="/usr/local/bin/python"

# --- Install Java (OpenJDK 11) ---
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# --- Install PySpark and dependencies from requirements.txt ---
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Copy all application code ---
COPY . /app

# Ensure data directories exist
RUN mkdir -p /app/data/raw /app/data/processed /app/data/metadata /app/data/static

# Initialize static lookup files once inside the container build
RUN python /app/scripts/init_lookups.py

# Define the entrypoint to run the Airflow DAG or the main script
# Using a shell script as the command is best practice for Docker
CMD ["/bin/bash", "entrypoint.sh"]