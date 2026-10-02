import json
from datetime import datetime, timezone

import pytest

import src.run_pipeline as pipeline
from src.compaction import CompactionSummary
from src.openalex_ingest import OpenAlexRequestError


@pytest.fixture(autouse=True)
def isolated_environment(tmp_path, monkeypatch):
    """Keep container .env values from reaching real APIs and point every zone at tmp_path."""
    for name in ("SCOPUS_QUERY", "SCOPUS_API", "INGEST_FIRST_YEAR", "INGEST_LAST_YEAR",
                 "INGEST_INITIAL_TARGET", "INCREMENTAL_LIMIT", "COMPACT_MIN_BATCHES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("BRONZE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))


@pytest.fixture(autouse=True)
def fake_spark(monkeypatch):
    """Replace Spark so these tests only exercise orchestration."""
    calls = {"processed": (), "compaction": [], "compacted": ()}

    class Builder:
        def master(self, value): return self
        def appName(self, value): return self
        def config(self, key, value): return self
        def getOrCreate(self): return type("Spark", (), {"stop": lambda self: None})()

    monkeypatch.setattr(pipeline.SparkSession, "builder", Builder())
    monkeypatch.setattr(pipeline, "compact_pending", lambda spark, bronze, silver, min_batches: calls["compaction"].append((bronze, silver, min_batches)) or CompactionSummary(calls["compacted"], ()))
    monkeypatch.setattr(pipeline, "process_pending_batches", lambda spark, bronze, silver: type(
        "Result", (), dict(processed_batches=calls["processed"], skipped_batches=(), valid_rows=0, rejected_rows=0))())
    return calls


def openalex_summary(status="completed_target", records=1):
    return type("OA", (), dict(batch_id="oa", mode="initial", status=status, records=records, pages=1))()


def scopus_summary(status="completed_target"):
    return type("SC", (), dict(batch_id="scopus-1", status=status, records=1))()


def test_scheduler_runs_both_sources_with_the_default_2011_to_current_year_range(monkeypatch, capsys):
    seen = {}
    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: seen.setdefault("openalex", kw) and [openalex_summary()])
    monkeypatch.setattr(pipeline, "ingest_scopus", lambda root, **kw: seen.setdefault("scopus", kw) and [scopus_summary("quota_exceeded")])
    pipeline.main()
    year = datetime.now(timezone.utc).year
    shared = {"first_year": 2011, "last_year": year, "initial_target": 100_000, "incremental_limit": 1000}
    assert seen["openalex"] == shared
    assert seen["scopus"] == {"query": "TITLE(test)", **shared}
    report = json.loads(capsys.readouterr().out)
    assert report["scopus_status"] == "quota_exceeded"
    assert report["event"] == "pipeline_partial"


def test_environment_overrides_range_and_budgets(monkeypatch):
    seen = {}
    for name, value in {"INGEST_FIRST_YEAR": "2015", "INGEST_LAST_YEAR": "2020",
                        "INGEST_INITIAL_TARGET": "500", "INCREMENTAL_LIMIT": "50"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: seen.update(kw) or [openalex_summary()])
    pipeline.main()
    assert seen == {"first_year": 2015, "last_year": 2020, "initial_target": 500, "incremental_limit": 50}


def test_scopus_is_skipped_without_a_configured_query(monkeypatch, capsys):
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary()])
    monkeypatch.setattr(pipeline, "ingest_scopus", lambda root, **kw: pytest.fail("Scopus must not run without a query"))
    pipeline.main()
    report = json.loads(capsys.readouterr().out)
    assert report["scopus_status"] is None and report["event"] == "pipeline_complete"


def test_openalex_rate_failure_does_not_prevent_scopus_and_pending_silver(monkeypatch, capsys):
    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: (_ for _ in ()).throw(OpenAlexRequestError("OpenAlex request failed", "rate_limited")))
    seen = []
    monkeypatch.setattr(pipeline, "ingest_scopus", lambda root, **kw: seen.append(kw["query"]) or [scopus_summary()])
    pipeline.main()
    assert seen == ["TITLE(test)"]
    report = json.loads(capsys.readouterr().out)
    assert report["openalex_status"] == "rate_limited" and report["event"] == "pipeline_partial"


def test_scopus_planning_failure_is_reported_without_stopping_silver(monkeypatch, capsys):
    from src.openalex_ingest import IngestionError

    monkeypatch.setenv("SCOPUS_QUERY", "TITLE(test)")
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary()])
    monkeypatch.setattr(pipeline, "ingest_scopus", lambda root, **kw: (_ for _ in ()).throw(IngestionError("Scopus year count failed (HTTP 403)")))
    pipeline.main()
    report = json.loads(capsys.readouterr().out)
    assert report["scopus_status"] == "failed" and report["event"] == "pipeline_partial"
    assert report["processed_batches"] == []


def test_report_sums_records_across_yearly_openalex_batches(monkeypatch, capsys):
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary(records=5), openalex_summary(records=7)])
    pipeline.main()
    assert json.loads(capsys.readouterr().out)["ingested_records"] == 12


def test_pipeline_stops_at_silver_and_reports_no_gold(monkeypatch, capsys):
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary()])
    pipeline.main()
    report = json.loads(capsys.readouterr().out)
    assert not hasattr(pipeline, "build_gold")
    assert not any("gold" in key for key in report)


def test_pipeline_compacts_silver_after_processing_with_the_default_threshold(tmp_path, monkeypatch, capsys, fake_spark):
    fake_spark["compacted"] = ("openalex-2024-0001",)
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary()])
    pipeline.main()
    assert fake_spark["compaction"] == [(tmp_path / "bronze", tmp_path / "silver", 10)]
    assert json.loads(capsys.readouterr().out)["compacted_groups"] == ["openalex-2024-0001"]


def test_compaction_threshold_comes_from_the_environment(tmp_path, monkeypatch, fake_spark):
    monkeypatch.setenv("COMPACT_MIN_BATCHES", "3")
    monkeypatch.setattr(pipeline, "ingest_openalex", lambda root, **kw: [openalex_summary()])
    pipeline.main()
    assert fake_spark["compaction"][0][2] == 3
