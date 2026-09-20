import json
from pathlib import Path

import pytest
import requests

from src.openalex_ingest import (
    OpenAlexClient,
    OpenAlexRequestError,
    TokenConfigurationError,
    run_ingestion,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class CursorSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        page = self.pages[params["cursor"]]
        if isinstance(page, Exception):
            raise page
        return FakeResponse(page)


def page(start, count, next_cursor):
    return {
        "meta": {"next_cursor": next_cursor},
        "results": [
            {
                "id": f"https://openalex.org/W{start + offset}",
                "title": f"Work {start + offset}",
                "publication_year": 2024,
                "publication_date": "2024-01-01",
                "topics": [{"id": "https://openalex.org/T1", "display_name": "Testing"}],
            }
            for offset in range(count)
        ],
    }


def test_initial_ingestion_paginates_and_persists_raw_pages_and_metadata(tmp_path):
    session = CursorSession(
        {
            "*": page(0, 200, "cursor-1"),
            "cursor-1": page(200, 200, "cursor-2"),
            "cursor-2": page(400, 50, None),
        }
    )
    client = OpenAlexClient("unit-test-token", session=session)

    summary = run_ingestion(
        tmp_path / "bronze",
        client=client,
        initial_target_records=450,
        page_size=200,
    )

    assert summary.mode == "initial"
    assert summary.records == 450
    assert summary.pages == 3
    assert [call["params"]["cursor"] for call in session.calls] == [
        "*",
        "cursor-1",
        "cursor-2",
    ]
    assert all(call["params"]["per-page"] <= 200 for call in session.calls)

    metadata = json.loads(summary.metadata_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "completed"
    assert metadata["record_count"] == 450
    assert len(list((tmp_path / "bronze" / "openalex" / summary.batch_id).glob("page-*.json"))) == 3
    assert "unit-test-token" not in summary.metadata_path.read_text(encoding="utf-8")


def test_restart_resumes_running_initial_batch_instead_of_starting_incremental(
    tmp_path,
):
    interrupted_session = CursorSession(
        {
            "*": page(0, 200, "cursor-1"),
            "cursor-1": requests.ConnectionError("temporary failure"),
        }
    )

    with pytest.raises(Exception):
        run_ingestion(
            tmp_path / "bronze",
            client=OpenAlexClient("unit-test-token", session=interrupted_session),
            initial_target_records=450,
            page_size=200,
        )

    metadata_files = list((tmp_path / "bronze" / "batch_metadata").glob("*.json"))
    assert len(metadata_files) == 1
    running_metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
    assert running_metadata["status"] == "running"
    assert running_metadata["mode"] == "initial"
    assert running_metadata["record_count"] == 200

    resume_session = CursorSession(
        {
            "cursor-1": page(200, 200, "cursor-2"),
            "cursor-2": page(400, 50, None),
        }
    )
    summary = run_ingestion(
        tmp_path / "bronze",
        client=OpenAlexClient("unit-test-token", session=resume_session),
        initial_target_records=450,
        page_size=200,
    )

    assert summary.mode == "initial"
    assert summary.records == 450
    assert len(metadata_files) == 1
    assert [call["params"]["cursor"] for call in resume_session.calls] == [
        "cursor-1",
        "cursor-2",
    ]


def test_incremental_ingestion_is_capped_at_one_thousand_records(tmp_path):
    initial_session = CursorSession({"*": page(0, 2, "incremental-start")})
    run_ingestion(
        tmp_path / "bronze",
        client=OpenAlexClient("unit-test-token", session=initial_session),
        initial_target_records=2,
        page_size=200,
    )

    incremental_pages = {}
    cursor = "incremental-start"
    for page_number in range(5):
        next_cursor = f"incremental-{page_number + 1}"
        incremental_pages[cursor] = page(2 + page_number * 200, 200, next_cursor)
        cursor = next_cursor
    incremental_session = CursorSession(incremental_pages)

    summary = run_ingestion(
        tmp_path / "bronze",
        client=OpenAlexClient("unit-test-token", session=incremental_session),
        initial_target_records=2,
        incremental_limit=1000,
        page_size=200,
    )

    assert summary.mode == "incremental"
    assert summary.records == 1000
    assert len(incremental_session.calls) == 5
    assert all(call["params"]["per-page"] <= 200 for call in incremental_session.calls)


def test_environment_client_requires_a_non_empty_token(monkeypatch):
    monkeypatch.delenv("OPENALEX_TOKEN", raising=False)

    with pytest.raises(TokenConfigurationError, match="OPENALEX_TOKEN"):
        OpenAlexClient.from_environment()

    monkeypatch.setenv("OPENALEX_TOKEN", "   ")
    with pytest.raises(TokenConfigurationError, match="OPENALEX_TOKEN"):
        OpenAlexClient.from_environment()


def test_request_errors_never_echo_the_api_token():
    class FailingSession:
        def get(self, url, *, params, timeout):
            raise requests.HTTPError(
                "https://api.openalex.org/works?api_key=unit-test-token"
            )

    with pytest.raises(OpenAlexRequestError) as error:
        OpenAlexClient("unit-test-token", session=FailingSession()).fetch_page(
            cursor="*",
            per_page=1,
        )

    assert "unit-test-token" not in str(error.value)
