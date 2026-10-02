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
        if url.endswith("/rate-limit"):
            return FakeResponse({"rate_limit": {"credits_remaining": 100000, "resets_at": "tomorrow"}})
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
    assert metadata["status"] == "completed_target"
    assert metadata["record_count"] == 450
    assert len(list((tmp_path / "bronze" / "openalex" / summary.batch_id).glob("page-*.parquet"))) == 3
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
            client=OpenAlexClient("unit-test-token", session=interrupted_session, max_retries=0),
            initial_target_records=450,
            page_size=200,
        )

    metadata_files = list((tmp_path / "bronze" / "batch_metadata").glob("*.json"))
    assert len(metadata_files) == 1
    running_metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
    assert running_metadata["status"] == "transient_error"
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


def test_exhaustion_before_target_is_success_and_does_not_refetch(tmp_path):
    bronze = tmp_path / "bronze"
    client = OpenAlexClient("secret", session=CursorSession({"*": page(0, 2, "next"), "next": page(2, 0, None)}))
    first = run_ingestion(bronze, client=client, initial_target_records=10)
    assert first.status == "completed_exhausted"
    assert first.records == 2
    assert run_ingestion(bronze, client=client, initial_target_records=10).status == "completed_exhausted"
    assert len(list((bronze / "batch_metadata").glob("*.json"))) == 1


def test_default_initial_target_is_source_allocation_and_incremental_limit_unchanged():
    from src.openalex_ingest import DEFAULT_INCREMENTAL_LIMIT, DEFAULT_INITIAL_TARGET

    assert DEFAULT_INITIAL_TARGET == 100_000
    assert DEFAULT_INCREMENTAL_LIMIT == 1_000


def test_empty_terminal_page_is_exhaustion(tmp_path):
    client = OpenAlexClient("secret", session=CursorSession({"*": page(0, 0, None)}))
    summary = run_ingestion(tmp_path, client=client, initial_target_records=5)
    assert summary.status == "completed_exhausted"
    assert summary.records == 0


def test_transient_failure_keeps_confirmed_cursor_and_resumes(tmp_path):
    bronze = tmp_path / "bronze"
    client = OpenAlexClient("secret", max_retries=0, session=CursorSession({
        "*": page(0, 2, "next"), "next": requests.ConnectionError("secret")
    }))
    with pytest.raises(OpenAlexRequestError):
        run_ingestion(bronze, client=client, initial_target_records=4)
    state_file = next((bronze / "batch_metadata").glob("*.json"))
    state = json.loads(state_file.read_text())
    assert state["status"] == "transient_error"
    assert state["next_cursor"] == "next"
    assert state["record_count"] == 2
    assert "secret" not in state_file.read_text()
    resumed = CursorSession({"next": page(2, 2, "later")})
    summary = run_ingestion(bronze, client=OpenAlexClient("secret", session=resumed), initial_target_records=4)
    assert summary.status == "completed_target"
    assert [call["params"]["cursor"] for call in resumed.calls] == ["next"]


def test_page_written_but_checkpoint_failed_is_replayed_without_duplicates(tmp_path, monkeypatch):
    from src.openalex_ingest import BatchStateStore

    original = BatchStateStore.write_metadata
    def fail_once(self, metadata):
        if metadata["record_count"] == 2 and not getattr(self, "failed_once", False):
            self.failed_once = True
            raise OSError("simulated crash")
        return original(self, metadata)

    monkeypatch.setattr(BatchStateStore, "write_metadata", fail_once)
    bronze = tmp_path / "bronze"
    with pytest.raises(OSError):
        run_ingestion(bronze, client=OpenAlexClient("secret", session=CursorSession({"*": page(0, 2, "next")})), initial_target_records=2)
    monkeypatch.setattr(BatchStateStore, "write_metadata", original)
    summary = run_ingestion(bronze, client=OpenAlexClient("secret", session=CursorSession({"*": page(0, 2, "next")})), initial_target_records=2)
    assert summary.records == 2
    assert len(list((bronze / "openalex" / summary.batch_id).glob("page-*.parquet"))) == 1


def test_nonempty_page_without_next_cursor_is_not_proven_exhausted(tmp_path):
    client = OpenAlexClient("secret", session=CursorSession({"*": page(0, 2, None)}))
    with pytest.raises(OpenAlexRequestError):
        run_ingestion(tmp_path, client=client, initial_target_records=10)
    state = json.loads(next((tmp_path / "batch_metadata").glob("*.json")).read_text())
    assert state["status"] == "failed"


def test_rate_limit_prevents_request_without_storing_key(tmp_path):
    class RateSession(CursorSession):
        def get(self, url, *, params, timeout):
            if url.endswith("/rate-limit"):
                return FakeResponse({"api_key": "secret", "rate_limit": {"credits_remaining": 0, "resets_at": "tomorrow"}})
            return super().get(url, params=params, timeout=timeout)

    session = RateSession({"*": page(0, 1, "next")})
    client = OpenAlexClient("secret", session=session)
    summary = run_ingestion(tmp_path, client=client, initial_target_records=3)
    assert summary.status == "quota_exceeded"
    assert session.calls == []
    state = json.loads(summary.metadata_path.read_text())
    assert state["next_attempt_at"] == "tomorrow"
    assert "secret" not in summary.metadata_path.read_text()


@pytest.mark.parametrize("code, expected", [(429, "rate_limited"), (503, "transient_error"), (401, "failed")])
def test_http_failure_state_is_not_completed(tmp_path, code, expected):
    class ErrorSession(CursorSession):
        def get(self, url, *, params, timeout):
            if url.endswith("/rate-limit"):
                return super().get(url, params=params, timeout=timeout)
            response = requests.Response()
            response.status_code = code
            response.url = "https://api.openalex.org/works?api_key=secret"
            raise requests.HTTPError("secret", response=response)

    with pytest.raises(OpenAlexRequestError):
        run_ingestion(tmp_path, client=OpenAlexClient("secret", session=ErrorSession({}), max_retries=0), initial_target_records=3)
    state_path = next((tmp_path / "batch_metadata").glob("*.json"))
    state = json.loads(state_path.read_text())
    assert state["status"] == expected
    assert state["http_status"] == code
    assert "secret" not in state_path.read_text()


def test_missing_pagination_metadata_is_not_exhaustion(tmp_path):
    client = OpenAlexClient("secret", session=CursorSession({"*": {"results": []}}))
    with pytest.raises(OpenAlexRequestError):
        run_ingestion(tmp_path, client=client, initial_target_records=3)
    state = json.loads(next((tmp_path / "batch_metadata").glob("*.json")).read_text())
    assert state["status"] == "failed"


def test_openalex_ignores_paused_scopus_batches(tmp_path):
    metadata_dir = tmp_path / "batch_metadata"
    metadata_dir.mkdir()
    (metadata_dir / "scopus.json").write_text(json.dumps({
        "batch_id": "scopus", "source": "scopus", "status": "completed_target",
        "mode": "initial", "target_records": 100, "created_at": "z",
    }))
    summary = run_ingestion(tmp_path, client=OpenAlexClient("secret", session=CursorSession({"*": page(0, 1, "next")})), initial_target_records=1)
    assert summary.status == "completed_target"
    assert summary.batch_id != "scopus"


def test_openalex_retries_transient_request_without_advancing_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr("src.openalex_ingest.time.sleep", lambda seconds: None)
    class FlakySession(CursorSession):
        def __init__(self):
            super().__init__({"*": page(0, 1, "next")})
            self.failures = 0

        def get(self, url, *, params, timeout):
            if url.endswith("/works") and self.failures < 2:
                self.failures += 1
                raise requests.Timeout("secret")
            return super().get(url, params=params, timeout=timeout)

    session = FlakySession()
    client = OpenAlexClient("secret", session=session, max_retries=2)
    summary = run_ingestion(tmp_path, client=client, initial_target_records=1)
    assert summary.status == "completed_target"
    assert session.failures == 2


def test_invalid_cursor_marks_batch_failed(tmp_path):
    client = OpenAlexClient("secret", session=CursorSession({"*": {"meta": {"next_cursor": 123}, "results": [{"id": "W1"}]}}))
    with pytest.raises(OpenAlexRequestError):
        run_ingestion(tmp_path, client=client, initial_target_records=2)
    metadata = json.loads(next((tmp_path / "batch_metadata").glob("*.json")).read_text())
    assert metadata["status"] == "failed"


def test_openalex_rate_limit_retries_after_header(tmp_path, monkeypatch):
    waits = []
    monkeypatch.setattr("src.openalex_ingest.time.sleep", waits.append)
    class RateOnce(CursorSession):
        def __init__(self):
            super().__init__({"*": page(0, 1, "next")})
            self.calls_to_works = 0

        def get(self, url, *, params, timeout):
            if url.endswith("/works"):
                self.calls_to_works += 1
                if self.calls_to_works == 1:
                    response = requests.Response()
                    response.status_code = 429
                    response.headers["Retry-After"] = "2"
                    raise requests.HTTPError("private", response=response)
            return super().get(url, params=params, timeout=timeout)

    session = RateOnce()
    summary = run_ingestion(tmp_path, client=OpenAlexClient("secret", session=session, max_retries=1), initial_target_records=1)
    assert summary.status == "completed_target"
    assert session.calls_to_works == 2
    assert waits == [2]


def test_client_sends_filter_only_when_configured():
    session = CursorSession({"*": page(0, 1, None)})
    OpenAlexClient("unit-test-token", session=session, filter_expression="title_and_abstract.search:(ai)").fetch_page(cursor="*", per_page=1)
    OpenAlexClient("unit-test-token", session=session).fetch_page(cursor="*", per_page=1)
    assert session.calls[0]["params"]["filter"] == "title_and_abstract.search:(ai)"
    assert "filter" not in session.calls[1]["params"]


def test_client_from_environment_uses_shared_ai_filter_unless_overridden(monkeypatch):
    from src.search_criteria import openalex_filter

    monkeypatch.setenv("OPENALEX_TOKEN", "unit-test-token")
    monkeypatch.delenv("OPENALEX_FILTER", raising=False)
    assert OpenAlexClient.from_environment().filter_expression == openalex_filter()
    monkeypatch.setenv("OPENALEX_FILTER", "type:article")
    assert OpenAlexClient.from_environment().filter_expression == "type:article"
    monkeypatch.setenv("OPENALEX_FILTER", "none")
    assert OpenAlexClient.from_environment().filter_expression == ""


def test_changed_filter_starts_a_new_initial_batch_instead_of_reusing_old_cursor(tmp_path):
    old = CursorSession({"*": page(0, 2, "old-cursor"), "old-cursor": page(2, 2, None)})
    run_ingestion(tmp_path / "bronze", client=OpenAlexClient("unit-test-token", session=old), initial_target_records=2, page_size=2)
    fresh = CursorSession({"*": page(100, 2, "new-cursor"), "new-cursor": page(102, 2, None)})
    filtered = OpenAlexClient("unit-test-token", session=fresh, filter_expression="title_and_abstract.search:(ai)")
    summary = run_ingestion(tmp_path / "bronze", client=filtered, initial_target_records=2, page_size=2)
    assert summary.mode == "initial"
    assert fresh.calls[0]["params"]["cursor"] == "*"
    metadata = json.loads(Path(summary.metadata_path).read_text())
    assert metadata["filter"] == "title_and_abstract.search:(ai)"


class YearSession:
    """Fake OpenAlex that serves per-year volumes with offset cursors and group_by totals."""

    def __init__(self, volumes):
        self.volumes = volumes
        self.calls = []

    def get(self, url, *, params, timeout):
        import re

        if url.endswith("/rate-limit"):
            return FakeResponse({"rate_limit": {"credits_remaining": 100000, "resets_at": "tomorrow"}})
        self.calls.append(dict(params))
        if "group_by" in params:
            return FakeResponse({"group_by": [{"key": str(y), "count": n} for y, n in self.volumes.items()]})
        year = int(re.search(r"publication_year:(\d{4})", params["filter"]).group(1))
        offset = 0 if params["cursor"] == "*" else int(params["cursor"])
        count = min(params["per-page"], self.volumes[year] - offset)
        results = [
            {"id": f"https://openalex.org/W{year}x{offset + i}", "title": "t", "publication_year": year,
             "publication_date": f"{year}-01-01", "topics": [{"id": "T", "display_name": "x"}]}
            for i in range(count)
        ]
        more = offset + count < self.volumes[year]
        return FakeResponse({"meta": {"next_cursor": str(offset + count) if more else None}, "results": results})


def _ingested_ids(root):
    import pyarrow.parquet as pq

    ids = []
    for path in (root / "openalex").glob("*/page-*.parquet"):
        ids += [json.loads(row["record_json"])["id"] for row in pq.read_table(path).to_pylist()]
    return ids


def _by_year(root):
    ids = _ingested_ids(root)
    return tuple(len([i for i in ids if f"W{year}x" in i]) for year in (2020, 2021))


def test_for_year_adds_the_publication_year_to_the_filter():
    base = OpenAlexClient("unit-test-token", session=CursorSession({}), filter_expression="title_and_abstract.search:(ai)")
    assert base.for_year(2015).filter_expression == "title_and_abstract.search:(ai),publication_year:2015"
    assert OpenAlexClient("unit-test-token", session=CursorSession({})).for_year(2015).filter_expression == "publication_year:2015"
    assert base.filter_expression == "title_and_abstract.search:(ai)"


def test_year_totals_use_one_group_by_request():
    session = YearSession({2020: 7, 2021: 9})
    client = OpenAlexClient("unit-test-token", session=session, filter_expression="title_and_abstract.search:(ai)")
    assert client.year_totals(2019, 2021) == {2019: 0, 2020: 7, 2021: 9}
    assert len(session.calls) == 1
    assert session.calls[0]["group_by"] == "publication_year"
    assert session.calls[0]["filter"] == "title_and_abstract.search:(ai),publication_year:2019-2021"


def _year_options(session):
    return dict(
        client=OpenAlexClient("unit-test-token", session=session), first_year=2020, last_year=2021,
        year_quotas={2020: 3, 2021: 2}, year_totals={2020: 10, 2021: 10}, page_size=1,
    )


def test_each_openalex_year_stops_at_its_initial_quota(tmp_path):
    from src.openalex_ingest import run_openalex_years

    run_openalex_years(tmp_path / "bronze", **_year_options(YearSession({2020: 10, 2021: 10})))
    assert _by_year(tmp_path / "bronze") == (3, 2)


def test_openalex_incremental_serves_the_least_filled_year_first(tmp_path):
    from src.openalex_ingest import run_openalex_years

    session = YearSession({2020: 10, 2021: 10})
    run_openalex_years(tmp_path / "bronze", **_year_options(session))
    run_openalex_years(tmp_path / "bronze", incremental_limit=2, **_year_options(session))
    assert _by_year(tmp_path / "bronze") == (3, 4)
    run_openalex_years(tmp_path / "bronze", incremental_limit=2, **_year_options(session))
    assert _by_year(tmp_path / "bronze") == (5, 4)


def test_time_budget_pauses_a_batch_and_the_next_run_resumes_without_duplicates(tmp_path, clock):
    class Ticking(YearSession):
        def get(self, url, *, params, timeout):
            clock.now += 1
            return super().get(url, params=params, timeout=timeout)

    session = Ticking({2020: 10})
    client = OpenAlexClient("unit-test-token", session=session).for_year(2020)
    options = dict(client=client, initial_target_records=10, page_size=2)
    paused = run_ingestion(tmp_path / "bronze", deadline=clock.now + 3.5, **options)
    assert paused.status == "time_budget_reached" and 0 < paused.records < 10
    done = run_ingestion(tmp_path / "bronze", **options)
    assert done.status == "completed_target" and done.records == 10
    ids = _ingested_ids(tmp_path / "bronze")
    assert len(ids) == len(set(ids)) == 10


def test_time_budget_stops_before_the_next_year_and_the_next_run_continues(tmp_path, clock):
    from src.openalex_ingest import run_openalex_years

    class Ticking(YearSession):
        def get(self, url, *, params, timeout):
            clock.now += 1
            return super().get(url, params=params, timeout=timeout)

    session = Ticking({2020: 10, 2021: 10})
    options = dict(first_year=2020, last_year=2021, year_quotas={2020: 4, 2021: 4},
                   year_totals={2020: 10, 2021: 10}, page_size=2)
    first = run_openalex_years(tmp_path / "bronze", client=OpenAlexClient("unit-test-token", session=session),
                               deadline=clock.now + 2.5, **options)
    assert first[-1].status == "time_budget_reached"
    assert _by_year(tmp_path / "bronze")[1] == 0
    run_openalex_years(tmp_path / "bronze", client=OpenAlexClient("unit-test-token", session=session), **options)
    assert _by_year(tmp_path / "bronze") == (4, 4)
