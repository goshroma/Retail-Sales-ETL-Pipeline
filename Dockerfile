# Dockerfile
# ---------------------------------------------------------------------------
# Containerizes the ETL pipeline. PySpark needs a JVM, so this builds on
# python:3.11-slim and adds a headless OpenJDK rather than reaching for a
# heavier pre-built Spark image -- keeps the image close to what main.py
# actually needs.
#
# Note: like the rest of this project's MySQL/PySpark execution paths, this
# Dockerfile is written but was not build-tested -- the sandbox this project
# was built in had no network egress, so `docker build` (which needs to pull
# the base image and apt/pip packages) couldn't be run there. The steps
# below follow the standard, well-documented pattern for a PySpark + JDK
# image and should build cleanly with normal internet access.
# ---------------------------------------------------------------------------
FROM python:3.11-slim

# PySpark requires a JVM.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Generate the synthetic dataset (idempotent, seeded) then run the pipeline.
CMD ["sh", "-c", "python src/generate_dataset.py && python main.py"]
