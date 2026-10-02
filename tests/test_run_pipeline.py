import json

import pytest

import src.run_pipeline as pipeline


@pytest.fixture(autouse=True)
def isolated_scopus_environment(monkeypatch):
    """Keep container .env values from routing tests to the real Scopus API."""
    for name in ("SCOPUS_QUERY", "SCOPUS_FIRST_YEAR", "SCOPUS_LAST_YEAR", "SCOPUS_API"):
        monkeypatch.delenv(name, raising=False)


def test_scheduler_runs_scopus_when_query_is_configured(tmp_path, monkeypatch, capsys):
    class Summary:
        batch_id = "scopus-1"
        status = "quota_exceeded"
        records = 1

    calls = []
    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setenv("BRONZE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))
    monkeypatch.setattr(pipeline, "run_scopus_ingestion", lambda root, query: calls.append(query) or Summary())
    monkeypatch.setattr(pipeline, "run_ingestion", lambda root: type("OpenAlex", (), dict(batch_id="oa", mode="initial", status="completed_target", records=1, pages=1))())
    class FakeSpark:
        def stop(self):
            pass
    class FakeBuilder:
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value): return self
        def getOrCreate(self): return FakeSpark()
    monkeypatch.setattr(pipeline.SparkSession, "builder", FakeBuilder())
    monkeypatch.setattr(pipeline, "process_pending_batches", lambda spark, bronze, silver: type("Result", (), dict(processed_batches=(), skipped_batches=(), valid_rows=0, rejected_rows=0))())
    pipeline.main()
    assert calls == ["TITLE(test)"]
    assert json.loads(capsys.readouterr().out)["scopus_status"] == "quota_exceeded"


def test_openalex_rate_failure_does_not_prevent_scopus_and_pending_silver(tmp_path, monkeypatch, capsys):
    from src.openalex_ingest import OpenAlexRequestError
    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setenv("BRONZE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))
    monkeypatch.setattr(pipeline, "run_ingestion", lambda root: (_ for _ in ()).throw(OpenAlexRequestError("OpenAlex request failed", "rate_limited")))
    seen = []
    monkeypatch.setattr(pipeline, "run_scopus_ingestion", lambda root, query: seen.append(query) or type("Summary", (), dict(batch_id="scopus", status="completed_target"))())
    class Builder:
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value): return self
        def getOrCreate(self): return type("Spark", (), {"stop": lambda self: None})()
    monkeypatch.setattr(pipeline.SparkSession, "builder", Builder())
    monkeypatch.setattr(pipeline, "process_pending_batches", lambda spark, bronze, silver: type("Result", (), dict(processed_batches=(), skipped_batches=(), valid_rows=0, rejected_rows=0))())
    pipeline.main()
    assert seen == ["TITLE(test)"]
    report = json.loads(capsys.readouterr().out)
    assert report["openalex_status"] == "rate_limited"
    assert report["event"] == "pipeline_partial"


def test_scheduler_partitions_scopus_query_by_year_when_range_is_set(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setenv("SCOPUS_FIRST_YEAR", "2020")
    monkeypatch.setenv("SCOPUS_LAST_YEAR", "2021")
    monkeypatch.setenv("BRONZE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))
    monkeypatch.setattr(pipeline, "run_ingestion", lambda root: type("OA", (), dict(batch_id="oa", mode="initial", status="completed_target", records=1, pages=1))())
    calls = []
    monkeypatch.setattr(pipeline, "run_scopus_years", lambda root, **kwargs: calls.append(kwargs) or [type("SC", (), dict(batch_id="s", status="result_window_reached"))()])
    class Builder:
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value): return self
        def getOrCreate(self): return type("Spark", (), {"stop": lambda self: None})()
    monkeypatch.setattr(pipeline.SparkSession, "builder", Builder())
    monkeypatch.setattr(pipeline, "process_pending_batches", lambda spark, bronze, silver: type("Result", (), dict(processed_batches=(), skipped_batches=(), valid_rows=0, rejected_rows=0))())
    pipeline.main()
    assert calls == [{"query": "TITLE(test)", "first_year": 2020, "last_year": 2021}]
    assert json.loads(capsys.readouterr().out)["scopus_status"] == "result_window_reached"
