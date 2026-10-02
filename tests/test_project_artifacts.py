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

    assert "/data/bronze" in source
    assert "/data/silver" in source
    assert "OPENALEX_TOKEN" not in source
    assert "batch metadata" in source.lower()
    assert "rejection" in source.lower()
