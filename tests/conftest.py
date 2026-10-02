import os

import pytest


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("openalex-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def clock(monkeypatch):
    """Deterministic time.monotonic: tests advance it explicitly instead of sleeping."""
    import time

    class Clock:
        now = 1000.0

        def __call__(self):
            return self.now

    fake = Clock()
    monkeypatch.setattr(time, "monotonic", fake)
    return fake
