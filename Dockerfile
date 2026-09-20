FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# PySpark needs a JVM; curl installs the pinned Docker-friendly scheduler binary.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates curl openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ARG SUPERCRONIC_VERSION=v0.2.29
RUN curl --fail --silent --show-error --location \
    "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    --output /usr/local/bin/supercronic \
    && chmod 0755 /usr/local/bin/supercronic

WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --requirement requirements.txt
COPY pyproject.toml ./pyproject.toml
COPY compose.yaml ./compose.yaml

COPY src ./src
COPY tests ./tests
COPY scheduler ./scheduler
COPY docker ./docker
COPY notebooks ./notebooks
COPY README.md ./README.md

RUN mkdir -p /data/bronze /data/silver

EXPOSE 8888
CMD ["sh", "/app/docker/entrypoint.sh"]
