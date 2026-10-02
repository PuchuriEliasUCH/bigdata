import json
import re
from datetime import date

import pyarrow.parquet as pq
import pytest
import requests

from src.scopus_ingest import (
    ScopusClient,
    run_scopus_ingestion,
    run_scopus_years,
    split_window,
    split_year_range,
    window_query,
)


class Response:
    def __init__(self, entries, total, headers=None, status=200):
        self.status_code = status
        self.headers = headers or {}
        self.entries = entries
        self.total = total

    def json(self):
        return {"search-results": {"opensearch:totalResults": str(self.total), "entry": self.entries}}


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.starts = []

    def get(self, url, *, params, headers, timeout):
        self.starts.append(params["start"])
        return next(self.responses)


def test_scopus_target_checkpoint_and_resume(tmp_path):
    session = Session([Response([{"eid": "a"}], 3), Response([], 3, status=429, headers={"X-ELS-Status": "QUOTA_EXCEEDED", "X-RateLimit-Reset": "1"})])
    client = ScopusClient("secret", session=session)
    stopped = run_scopus_ingestion(tmp_path, client=client, query="TITLE(test)", target_records=3, page_size=1)
    assert stopped.status == "quota_exceeded"
    metadata = json.loads(stopped.metadata_path.read_text())
    assert metadata["mode"] == "initial"
    assert metadata["next_offset"] == 1
    assert metadata["next_attempt_at"] == "1"
    assert "secret" not in stopped.metadata_path.read_text()
    assert len(list((tmp_path / "scopus" / stopped.batch_id).glob("page-*.parquet"))) == 1
    resumed = Session([Response([{"eid": "b"}], 3), Response([{"eid": "c"}], 3)])
    done = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=resumed), query="TITLE(test)", target_records=3, page_size=1)
    assert done.status == "completed_target"
    assert resumed.starts == [1, 2]
    assert len(list((tmp_path / "scopus" / done.batch_id).glob("page-*.parquet"))) == 3


def test_scopus_window_is_not_exhaustion(tmp_path):
    session = Session([Response([{"eid": "a"}], 10), Response([{"eid": "b"}], 10)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=5, page_size=1, result_window=2)
    assert result.status == "result_window_reached"
    assert result.records == 2


def test_scopus_unmarked_429_is_not_quota(tmp_path):
    session = Session([Response([], 1, status=429)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=1, max_retries=0)
    assert result.status == "rate_limited"
    assert result.records == 0
    assert json.loads(result.metadata_path.read_text())["stop_reason"] == "http_429_unclassified"


def test_scopus_invalid_credentials_never_leak(tmp_path):
    session = Session([Response([], 1, status=401)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=1)
    assert result.status == "failed"
    assert "secret" not in result.metadata_path.read_text()


def test_scopus_client_reads_existing_environment_variable(monkeypatch):
    monkeypatch.setenv("SCOPUS_API", "private-token")
    monkeypatch.delenv("SCOPUS_API_KEY", raising=False)
    client = ScopusClient.from_environment()
    session = Session([Response([], 0)])
    client._session = session
    client.fetch_page("TITLE(test)", 0, 1)
    assert session.starts == [0]


def test_scopus_recovers_from_one_rate_limit_without_skipping_page(tmp_path, monkeypatch):
    monkeypatch.setattr("src.scopus_ingest.time.sleep", lambda seconds: None)
    session = Session([Response([], 1, status=429, headers={"Retry-After": "0"}), Response([{"eid": "a"}], 1)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=1, max_retries=1)
    assert result.status == "completed_target"
    assert session.starts == [0, 0]


def test_year_subdivision_is_nonoverlapping_and_complete():
    assert split_year_range("TITLE(test)", 2020, 2022) == [
        "(TITLE(test)) AND PUBYEAR = 2020",
        "(TITLE(test)) AND PUBYEAR = 2021",
        "(TITLE(test)) AND PUBYEAR = 2022",
    ]


def test_quota_pause_does_not_request_before_reset(tmp_path):
    client = ScopusClient("secret", session=Session([Response([], 2, status=429, headers={"X-ELS-Status": "QUOTA_EXCEEDED", "X-RateLimit-Reset": "9999999999"})]))
    first = run_scopus_ingestion(tmp_path, client=client, query="TITLE(test)", target_records=2)
    session = Session([])
    second = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=2)
    assert first.status == second.status == "quota_exceeded"
    assert session.starts == []


@pytest.mark.parametrize("response, status", [
    (Response([], 1, status=503), "transient_error"),
    (Response([], 1, status=200), "failed"),
])
def test_scopus_server_and_invalid_page_not_completed(tmp_path, response, status):
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=Session([response])), query="TITLE(test)", target_records=1, max_retries=0)
    assert result.status == status


def test_yearly_scopus_resume_skips_completed_partition(tmp_path):
    session = Session([Response([{"eid": "a"}], 1), Response([], 1, status=429, headers={"X-ELS-Status": "QUOTA_EXCEEDED", "X-RateLimit-Reset": "1"})])
    first = run_scopus_years(tmp_path, query="TITLE(test)", first_year=2020, last_year=2021, client=ScopusClient("secret", session=session), target_records=1)
    assert [summary.status for summary in first] == ["completed_target", "quota_exceeded"]
    resumed = Session([Response([{"eid": "b"}], 1)])
    second = run_scopus_years(tmp_path, query="TITLE(test)", first_year=2020, last_year=2021, client=ScopusClient("secret", session=resumed), target_records=1)
    assert [summary.status for summary in second] == ["completed_target", "completed_target"]
    assert resumed.starts == [0]
    assert len(list((tmp_path / "batch_metadata").glob("*.json"))) == 2


def test_scopus_retries_timeout_without_advancing_offset(tmp_path, monkeypatch):
    monkeypatch.setattr("src.scopus_ingest.time.sleep", lambda seconds: None)
    class FlakySession(Session):
        def get(self, url, *, params, headers, timeout):
            self.starts.append(params["start"])
            response = next(self.responses)
            if isinstance(response, Exception):
                raise response
            return response

    session = FlakySession([requests.Timeout("private"), Response([{"eid": "a"}], 1)])
    summary = run_scopus_ingestion(tmp_path, query="TITLE(test)", client=ScopusClient("secret", session=session), target_records=1, max_retries=1)
    assert summary.status == "completed_target"
    assert session.starts == [0, 0]


def test_scopus_honors_retry_after_http_date(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr("src.scopus_ingest.time.sleep", sleeps.append)
    monkeypatch.setattr("src.scopus_ingest.time.time", lambda: 0)
    session = Session([Response([], 1, status=429, headers={"Retry-After": "Thu, 01 Jan 1970 00:00:10 GMT"}), Response([{"eid": "a"}], 1)])
    result = run_scopus_ingestion(tmp_path, query="TITLE(test)", client=ScopusClient("secret", session=session), target_records=1, max_retries=1, requests_per_second=0)
    assert result.status == "completed_target"
    assert 10 in sleeps


def test_scopus_next_batch_continues_from_completed_target(tmp_path):
    first = run_scopus_ingestion(tmp_path, query="TITLE(test)", client=ScopusClient("secret", session=Session([Response([{"eid": "a"}], 2)])), target_records=1)
    session = Session([Response([{"eid": "b"}], 2)])
    second = run_scopus_ingestion(tmp_path, query="TITLE(test)", client=ScopusClient("secret", session=session), target_records=1)
    assert first.batch_id != second.batch_id
    assert session.starts == [1]
    assert json.loads(second.metadata_path.read_text())["mode"] == "incremental"


def test_scopus_uses_separate_persisted_initial_and_incremental_targets(tmp_path):
    initial = run_scopus_ingestion(
        tmp_path, query="TITLE(test)", client=ScopusClient("secret", session=Session([
            Response([], 1, status=429, headers={"X-ELS-Status": "QUOTA_EXCEEDED", "X-RateLimit-Reset": "9999999999"}),
        ])),
    )
    state = json.loads(initial.metadata_path.read_text())
    assert state["target_records"] == 100_000
    assert state["mode"] == "initial"



def test_year_partitions_share_initial_allocation_and_checkpoint(tmp_path):
    session = Session([Response([{"eid": "a"}], 2), Response([], 2, status=429, headers={"X-ELS-Status": "QUOTA_EXCEEDED", "X-RateLimit-Reset": "1"})])
    summaries = run_scopus_years(
        tmp_path, query="TITLE(test)", first_year=2020, last_year=2020,
        client=ScopusClient("secret", session=session), initial_target_records=2, page_size=1,
    )
    state = json.loads(summaries[-1].metadata_path.read_text())
    assert state["target_records"] == 2
    assert state["next_offset"] == 1


def test_year_partitions_share_one_initial_allocation(tmp_path):
    session = Session([
        Response([{"eid": "a"}], 3),
        Response([{"eid": "b"}], 3),
        Response([{"eid": "c"}], 3),
    ])
    summaries = run_scopus_years(
        tmp_path, query="TITLE(test)", first_year=2020, last_year=2021,
        client=ScopusClient("secret", session=session), initial_target_records=2, page_size=1,
    )
    initial = [json.loads(path.read_text()) for path in (tmp_path / "batch_metadata").glob("*.json")]
    assert sum(record["record_count"] for record in initial if record["mode"] == "initial") <= 2
    assert session.starts == [0, 1]
    assert len(summaries) == 1


def test_scopus_default_targets_are_distinct():
    import inspect

    parameters = inspect.signature(run_scopus_ingestion).parameters
    assert parameters["initial_target_records"].default == 100_000
    assert parameters["target_records"].default is None


def test_year_orchestrator_keeps_paging_year_before_advancing(tmp_path):
    session = Session([Response([{"eid": "a"}], 2), Response([{"eid": "b"}], 2), Response([{"eid": "c"}], 1)])
    summaries = run_scopus_years(tmp_path, query="TITLE(test)", first_year=2020, last_year=2021, client=ScopusClient("secret", session=session), target_records=1)
    assert session.starts == [0, 1, 0]
    assert len(summaries) == 3


EMPTY_RESULT_ENTRY = {"@_fa": "true", "error": "Result set was empty"}


class CorpusSession:
    """Fake Scopus API that evaluates PUBYEAR and ORIG-LOAD-DATE AFT/BEF predicates."""

    def __init__(self, corpus):
        self.corpus = corpus  # (eid, publication year, load date as YYYYMMDD int)
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        query = params["query"]
        self.calls.append(query)
        year = int(re.search(r"PUBYEAR = (\d{4})", query).group(1))
        after = re.search(r"ORIG-LOAD-DATE AFT (\d{8})", query)
        before = re.search(r"ORIG-LOAD-DATE BEF (\d{8})", query)
        rows = [
            eid for eid, y, loaded in self.corpus
            if y == year
            and (not after or loaded > int(after.group(1)))
            and (not before or loaded < int(before.group(1)))
        ]
        start, count = params["start"], params["count"]
        if not rows:
            # Scopus answers an empty result set with one error entry instead of no entries.
            return Response([EMPTY_RESULT_ENTRY], 0)
        return Response([{"eid": eid} for eid in rows[start:start + count]], len(rows))


def ingested_eids(root):
    eids = []
    for page in (root / "scopus").glob("*/page-*.parquet"):
        eids += [json.loads(row["record_json"])["eid"] for row in pq.read_table(page).to_pylist()]
    return eids


def _leaves(lo, hi, ceiling, depth):
    children = split_window(lo, hi, ceiling) if depth else []
    if not children:
        return [(lo, hi)]
    return [leaf for child in children for leaf in _leaves(*child, ceiling, depth - 1)]


def test_window_query_keeps_whole_history_query_unchanged():
    assert window_query("(TITLE(test)) AND PUBYEAR = 2020", None, None) == "(TITLE(test)) AND PUBYEAR = 2020"


def test_window_query_uses_exclusive_load_date_bounds():
    query = window_query("Q", date(2020, 1, 1), date(2020, 1, 3))
    assert query == "(Q) AND ORIG-LOAD-DATE AFT 20191231 AND ORIG-LOAD-DATE BEF 20200104"
    assert window_query("Q", None, date(2020, 1, 3)) == "(Q) AND ORIG-LOAD-DATE BEF 20200104"
    assert window_query("Q", date(2020, 1, 1), None) == "(Q) AND ORIG-LOAD-DATE AFT 20191231"


def test_split_window_is_disjoint_and_exhaustive_over_all_history():
    ceiling = date(2021, 6, 1)
    leaves = _leaves(None, None, ceiling, depth=6)
    assert leaves[0][0] is None and leaves[-1][1] is None
    for (_, previous_hi), (next_lo, _) in zip(leaves, leaves[1:]):
        assert (next_lo - previous_hi).days == 1


def test_split_window_stops_at_a_single_day():
    assert split_window(date(2020, 6, 15), date(2020, 6, 15), date(2021, 6, 1)) == []


def test_oversized_year_is_bisected_until_every_window_fits(tmp_path):
    corpus = [(f"e{i}", 2020, loaded) for i, loaded in enumerate(
        [20191230, 20200105, 20200110, 20200301, 20200302, 20200615,
         20200901, 20201015, 20201201, 20210101, 20210301, 20210315])]
    session = CorpusSession(corpus)
    summaries = run_scopus_years(
        tmp_path, query="TITLE(test)", first_year=2020, last_year=2020,
        client=ScopusClient("secret", session=session), result_window=5, page_size=5,
        requests_per_second=0, ceiling=date(2021, 6, 1),
    )
    assert sorted(ingested_eids(tmp_path)) == sorted(eid for eid, _, _ in corpus)
    assert all(summary.status != "oversized" for summary in summaries)


def test_capped_single_day_does_not_block_the_next_year_or_loop_on_rerun(tmp_path):
    corpus = [(f"a{i}", 2020, 20200615) for i in range(8)] + [("b0", 2021, 20210101), ("b1", 2021, 20210102)]
    session = CorpusSession(corpus)
    options = dict(
        query="TITLE(test)", first_year=2020, last_year=2021,
        client=ScopusClient("secret", session=session), result_window=5, page_size=5,
        requests_per_second=0, ceiling=date(2021, 6, 1),
    )
    first = run_scopus_years(tmp_path, **options)
    assert {"b0", "b1"} <= set(ingested_eids(tmp_path))
    assert "result_window_reached" in {summary.status for summary in first}
    requests_after_first = len(session.calls)
    run_scopus_years(tmp_path, **options)
    assert len(session.calls) == requests_after_first


def test_incremental_runs_continue_after_initial_target_until_results_end(tmp_path):
    corpus = [(f"e{i}", 2020, 20200101 + i) for i in range(5)]
    session = CorpusSession(corpus)
    options = dict(
        query="TITLE(test)", first_year=2020, last_year=2020,
        client=ScopusClient("secret", session=session), page_size=1,
        requests_per_second=0, ceiling=date(2021, 6, 1),
    )
    run_scopus_years(tmp_path, initial_target_records=2, **options)
    assert len(ingested_eids(tmp_path)) == 2
    run_scopus_years(tmp_path, initial_target_records=2, target_records=2, **options)
    assert len(ingested_eids(tmp_path)) == 4
    run_scopus_years(tmp_path, initial_target_records=2, target_records=2, **options)
    assert sorted(ingested_eids(tmp_path)) == sorted(eid for eid, _, _ in corpus)
    calls = len(session.calls)
    run_scopus_years(tmp_path, initial_target_records=2, target_records=2, **options)
    assert len(session.calls) == calls


def _yearly_corpus():
    return [(f"a{i}", 2020, 20200101 + i) for i in range(10)] + [(f"b{i}", 2021, 20210101 + i) for i in range(10)]


def _count_by_year(root):
    eids = ingested_eids(root)
    return len([e for e in eids if e.startswith("a")]), len([e for e in eids if e.startswith("b")])


def _yearly_options(session):
    return dict(
        query="TITLE(test)", first_year=2020, last_year=2021,
        client=ScopusClient("secret", session=session), page_size=1, requests_per_second=0,
        ceiling=date(2021, 6, 1), initial_target_records=5,
        year_quotas={2020: 3, 2021: 2}, year_totals={2020: 10, 2021: 10},
    )


def test_each_year_stops_at_its_own_initial_quota(tmp_path):
    run_scopus_years(tmp_path, **_yearly_options(CorpusSession(_yearly_corpus())))
    assert _count_by_year(tmp_path) == (3, 2)


def test_incremental_runs_serve_the_least_filled_year_first(tmp_path):
    session = CorpusSession(_yearly_corpus())
    options = _yearly_options(session)
    run_scopus_years(tmp_path, **options)
    run_scopus_years(tmp_path, target_records=2, **options)
    assert _count_by_year(tmp_path) == (3, 4)  # 2021 is 20% full, 2020 is 30% full
    run_scopus_years(tmp_path, target_records=2, **options)
    assert _count_by_year(tmp_path) == (5, 4)  # now 2020 (30%) is behind 2021 (40%)


def test_year_quota_run_is_idempotent_before_incremental_budget_is_granted(tmp_path):
    session = CorpusSession(_yearly_corpus())
    run_scopus_years(tmp_path, **_yearly_options(session))
    assert len(ingested_eids(tmp_path)) == 5


def test_empty_result_set_error_entry_is_not_stored_as_a_record(tmp_path):
    session = Session([Response([EMPTY_RESULT_ENTRY], 0)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=5)
    assert result.status == "completed_exhausted"
    assert result.records == 0
    assert not list((tmp_path / "scopus").glob("*/page-*"))
    assert json.loads(result.metadata_path.read_text())["page_count"] == 0


def test_zero_total_never_yields_records_even_if_entries_are_present(tmp_path):
    session = Session([Response([{"eid": "ghost"}], 0)])
    result = run_scopus_ingestion(tmp_path, client=ScopusClient("secret", session=session), query="TITLE(test)", target_records=5)
    assert result.records == 0
    assert ingested_eids(tmp_path) == []


def test_empty_windows_do_not_consume_the_year_quota(tmp_path):
    corpus = [(f"a{i}", 2020, 20200101 + i) for i in range(6)]
    session = CorpusSession(corpus)
    run_scopus_years(
        tmp_path, query="TITLE(test)", first_year=2020, last_year=2020,
        client=ScopusClient("secret", session=session), result_window=3, page_size=3,
        requests_per_second=0, ceiling=date(2021, 6, 1), year_quotas={2020: 6}, year_totals={2020: 6},
        initial_target_records=6,
    )
    assert sorted(ingested_eids(tmp_path)) == sorted(eid for eid, _, _ in corpus)
