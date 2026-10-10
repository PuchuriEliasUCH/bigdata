import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_compose_has_one_scheduled_pipeline_service_and_env_file():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    assert "services:" in compose
    assert "spark-pipeline:" in compose
    assert "env_file:" in compose
    assert "- .env" in compose
    assert "*/2 * * * *" in (ROOT / "scheduler" / "pipeline.cron").read_text(
        encoding="utf-8"
    )


def test_notebook_uses_mounted_zones_without_token_access():
    notebook = json.loads(
        (ROOT / "notebooks" / "openalex_pipeline_evidence.ipynb").read_text(
            encoding="utf-8"
        )
    )
    source = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )

    # Inside the container the zones come from the /data mount; elsewhere from the checkout's data/.
    assert 'Path("/data")' in source
    assert 'DATA / "bronze"' in source
    assert 'DATA / "silver"' in source
    assert "OPENALEX_TOKEN" not in source
    assert "batch metadata" in source.lower()
    assert "rejection" in source.lower()


def test_notebook_reflects_current_pipeline_outputs():
    notebook = json.loads(
        (ROOT / "notebooks" / "openalex_pipeline_evidence.ipynb").read_text(
            encoding="utf-8"
        )
    )
    source = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )

    # Bronze pages are Parquet; a JSON-only glob would always report zero pages.
    assert "page-*.parquet" in source
    # Spark DataFrames are not rendered by display() without pandas.
    assert "display(" not in source
    # Silver schema v3 columns and Scopus window bisection must be visible.
    for expected in ("topics", "authors", "country_codes", "has_abstract", "stop_reason", "oversized"):
        assert expected in source
    # Compacted rejected rows must be read too.
    assert "active_rejected_paths" in source
