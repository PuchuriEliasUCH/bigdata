import json

import pyarrow.parquet as pq
import pytest

from src import ingestion
from src.openalex_ingest import IngestionError, OpenAlexClient
from src.scopus_ingest import ScopusClient


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self.payload, self.status_code, self.headers = payload, status, headers or {}

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class OpenAlexYears:
    def __init__(self, volumes):
        self.volumes, self.calls = volumes, []

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
        results = [{"id": f"W{year}x{offset + i}", "title": "t", "publication_year": year, "publication_date": f"{year}-01-01",
                    "topics": [{"id": "T", "display_name": "x"}]} for i in range(count)]
        more = offset + count < self.volumes[year]
        return FakeResponse({"meta": {"next_cursor": str(offset + count) if more else None}, "results": results})


class ScopusYears:
    def __init__(self, volumes, status=200):
        self.volumes, self.status, self.calls = volumes, status, []

    def get(self, url, *, params, headers, timeout):
        import re

        self.calls.append(params["query"])
        if self.status != 200:
            return FakeResponse({}, status=self.status)
        year = int(re.search(r"PUBYEAR = (\d{4})", params["query"]).group(1))
        start, count = params["start"], params["count"]
        total = self.volumes[year]
        entries = [{"eid": f"{year}-{i}"} for i in range(start, min(total, start + count))]
        return FakeResponse({"search-results": {"opensearch:totalResults": str(total), "entry": entries}})


def _counts(root, source):
    ids = []
    for page in (root / source).glob("*/page-*.parquet"):
        for row in pq.read_table(page).to_pylist():
            record = json.loads(row["record_json"])
            ids.append(record.get("id") or record["eid"])
    return tuple(len([i for i in ids if str(year) in i.split("x")[0].split("-")[0]]) for year in (2020, 2021))


def test_openalex_quotas_follow_volume_and_the_plan_is_computed_once(tmp_path, monkeypatch):
    session = OpenAlexYears({2020: 30, 2021: 10})
    monkeypatch.setattr(ingestion.OpenAlexClient, "from_environment", classmethod(lambda cls: OpenAlexClient("t", session=session)))
    options = dict(first_year=2020, last_year=2021, initial_target=8, incremental_limit=2, page_size=1)
    ingestion.ingest_openalex(tmp_path, **options)
    assert _counts(tmp_path, "openalex") == (6, 2)
    ingestion.ingest_openalex(tmp_path, **options)
    assert len([call for call in session.calls if "group_by" in call]) == 1


def test_scopus_quotas_follow_volume(tmp_path, monkeypatch):
    session = ScopusYears({2020: 30, 2021: 10})
    monkeypatch.setattr(ingestion.ScopusClient, "from_environment", classmethod(lambda cls: ScopusClient("secret", session=session)))
    ingestion.ingest_scopus(tmp_path, query="TITLE(t)", first_year=2020, last_year=2021, initial_target=8,
                            incremental_limit=2, page_size=1, requests_per_second=0)
    assert _counts(tmp_path, "scopus") == (6, 2)


def test_scopus_plan_failure_is_sanitized_and_does_not_start_ingestion(tmp_path, monkeypatch):
    session = ScopusYears({2020: 1}, status=403)
    monkeypatch.setattr(ingestion.ScopusClient, "from_environment", classmethod(lambda cls: ScopusClient("secret", session=session)))
    with pytest.raises(IngestionError) as error:
        ingestion.ingest_scopus(tmp_path, query="TITLE(t)", first_year=2020, last_year=2020, initial_target=5,
                                incremental_limit=2, requests_per_second=0)
    assert "secret" not in str(error.value) and "403" in str(error.value)
    assert not (tmp_path / "scopus").exists()
