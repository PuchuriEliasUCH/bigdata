import json
from datetime import datetime
from pathlib import Path

import pytest

from src.freeze_batches import freeze_paused_batches


def write_metadata(bronze_root: Path, batch_id: str, status: str, source: str, page_count: int = 1) -> Path:
    metadata_dir = bronze_root / "batch_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    path = metadata_dir / f"{batch_id}.json"
    path.write_text(json.dumps({"batch_id": batch_id, "source": source, "status": status, "page_count": page_count}), encoding="utf-8")
    return path


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "source, status",
    [
        ("openalex", "time_budget_reached"),
        ("openalex", "rate_limited"),
        ("openalex", "transient_error"),
        ("openalex", "running"),
        ("scopus", "time_budget_reached"),
        ("scopus", "quota_exceeded"),
        ("scopus", "rate_limited"),
        ("scopus", "transient_error"),
        ("scopus", "running"),
    ],
)
def test_freeze_ends_paused_batches_with_pages(tmp_path, source, status):
    path = write_metadata(tmp_path, "b1", status, source)

    assert freeze_paused_batches(tmp_path) == ["b1"]

    record = read(path)
    assert record["status"] == "frozen"
    assert record["frozen_from"] == status
    assert datetime.fromisoformat(record["frozen_at"]).tzinfo is not None
    assert record["page_count"] == 1


@pytest.mark.parametrize(
    "source, status, page_count",
    [
        ("openalex", "time_budget_reached", 0),
        ("scopus", "time_budget_reached", 0),
        ("openalex", "completed_target", 1),
        ("openalex", "quota_exceeded", 1),
        ("openalex", "failed", 1),
        ("scopus", "completed_exhausted", 1),
        ("scopus", "result_window_reached", 1),
        ("scopus", "oversized", 1),
    ],
)
def test_freeze_leaves_empty_and_terminal_batches_alone(tmp_path, source, status, page_count):
    path = write_metadata(tmp_path, "b1", status, source, page_count)
    before = path.read_text(encoding="utf-8")

    assert freeze_paused_batches(tmp_path) == []
    assert path.read_text(encoding="utf-8") == before


def test_freeze_is_idempotent_and_keeps_the_original_status(tmp_path):
    path = write_metadata(tmp_path, "b1", "time_budget_reached", "openalex")
    assert freeze_paused_batches(tmp_path) == ["b1"]
    first = read(path)

    assert freeze_paused_batches(tmp_path) == []
    assert read(path) == first
    assert first["frozen_from"] == "time_budget_reached"


def test_freeze_without_metadata_directory_is_a_no_op(tmp_path):
    assert freeze_paused_batches(tmp_path / "missing") == []
