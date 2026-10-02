"""Recoverable Scopus Search ingestion with bounded result-window pagination."""

from __future__ import annotations

import os
import time
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from src.openalex_ingest import BatchStateStore, BatchStateError, IngestionError, _batch_id, utc_now


SCOPUS_URL = "https://api.elsevier.com/content/search/scopus"
DEFAULT_INITIAL_TARGET = 100_000
DEFAULT_INCREMENTAL_LIMIT = 1_000


def split_year_range(query: str, first_year: int, last_year: int) -> list[str]:
    """Build disjoint Scopus year predicates for a window-limited search."""
    if not query or first_year > last_year:
        raise ValueError("A query and ascending year range are required")
    return [f"({query}) AND PUBYEAR = {year}" for year in range(first_year, last_year + 1)]


@dataclass(frozen=True)
class ScopusSummary:
    batch_id: str
    status: str
    records: int
    metadata_path: Path


class ScopusClient:
    def __init__(self, api_key: str, *, session: Any = None, timeout_seconds: int = 60):
        if not api_key or not api_key.strip():
            raise IngestionError("SCOPUS_API must be set")
        self._key = api_key.strip()
        self._session = session or requests.Session()
        self._timeout = timeout_seconds

    @classmethod
    def from_environment(cls) -> "ScopusClient":
        return cls(os.environ.get("SCOPUS_API", ""))

    def fetch_page(self, query: str, start: int, count: int) -> Any:
        return self._session.get(
            SCOPUS_URL,
            params={"query": query, "start": start, "count": count},
            headers={"X-ELS-APIKey": self._key, "Accept": "application/json"},
            timeout=self._timeout,
        )


def run_scopus_ingestion(
    bronze_root: str | Path,
    *,
    query: str,
    client: ScopusClient | None = None,
    target_records: int | None = None,
    initial_target_records: int = DEFAULT_INITIAL_TARGET,
    page_size: int = 25,
    result_window: int = 5000,
    requests_per_second: float = 2.0,
    max_retries: int = 2,
) -> ScopusSummary:
    if not query or (target_records is not None and target_records < 1) or initial_target_records < 1 or not 1 <= page_size <= 200 or result_window < 1 or max_retries < 0:
        raise IngestionError("Invalid Scopus query or pagination settings")
    store = BatchStateStore(Path(bronze_root), "scopus")
    api = client or ScopusClient.from_environment()
    with store.lock():
        history = [m for m in store.list_metadata() if m.get("source") == "scopus" and m.get("query") == query]
        candidates = [m for m in history if m.get("status") not in {"completed_target", "completed_exhausted", "failed"}]
        if len(candidates) > 1:
            raise BatchStateError("Multiple Scopus batches match this query")
        previous = max((m for m in history if m.get("status") in {"completed_target", "completed_exhausted"}), key=lambda m: m.get("created_at", ""), default=None)
        if not candidates and previous and (previous["status"] == "completed_exhausted" or previous.get("next_offset", 0) >= previous.get("total_results", float("inf"))):
            return ScopusSummary(previous["batch_id"], "completed_exhausted", previous["record_count"], previous["_metadata_path"])
        mode = candidates[0].get("mode") if candidates else ("incremental" if previous else "initial")
        if mode not in {"initial", "incremental"}:
            raise BatchStateError("Scopus batch mode is invalid")
        configured_target = (initial_target_records if target_records is None else target_records) if mode == "initial" else (target_records or DEFAULT_INCREMENTAL_LIMIT)
        metadata = candidates[0] if candidates else {
            "batch_id": _batch_id("scopus"), "source": "scopus", "query": query, "mode": mode,
            "status": "running", "stop_reason": None, "record_count": 0,
            "page_count": 0, "next_offset": previous["next_offset"] if previous else 0, "target_records": configured_target,
            "raw_page_dir": "", "raw_page_files": [], "created_at": utc_now(),
        }
        metadata["raw_page_dir"] = f"scopus/{metadata['batch_id']}"
        store.write_metadata(metadata)
        if metadata["status"] == "quota_exceeded" and metadata.get("next_attempt_at"):
            try:
                if time.time() < float(metadata["next_attempt_at"]):
                    return ScopusSummary(metadata["batch_id"], "quota_exceeded", metadata["record_count"], store.metadata_dir / f"{metadata['batch_id']}.json")
            except ValueError:
                pass
        last_request = 0.0
        while metadata["record_count"] < metadata["target_records"]:
            offset = metadata["next_offset"]
            if offset >= result_window:
                metadata["status"] = "result_window_reached"
                break
            count = min(page_size, metadata["target_records"] - metadata["record_count"], result_window - offset)
            wait = (1 / requests_per_second) - (time.monotonic() - last_request) if requests_per_second > 0 else 0
            if wait > 0:
                time.sleep(wait)
            retries = 0
            try:
                while True:
                    try:
                        response = api.fetch_page(query, offset, count)
                    except (requests.Timeout, requests.ConnectionError):
                        if retries < max_retries:
                            retries += 1
                            time.sleep(retries)
                            continue
                        raise
                    last_request = time.monotonic()
                    code = response.status_code
                    headers = response.headers
                    if code == 429 and headers.get("X-ELS-Status") != "QUOTA_EXCEEDED" and retries < max_retries:
                        retries += 1
                        delay = headers.get("Retry-After")
                        try:
                            seconds = float(delay) if delay is not None else float(retries)
                        except ValueError:
                            try:
                                seconds = parsedate_to_datetime(delay).timestamp() - time.time()
                            except (TypeError, ValueError, OverflowError):
                                seconds = float(retries)
                        time.sleep(min(max(0, seconds), 60))
                        continue
                    if code >= 500 and retries < max_retries:
                        retries += 1
                        time.sleep(retries)
                        continue
                    break
                metadata["http_status"] = code
                for header, field in (("X-RateLimit-Limit", "rate_limit"), ("X-RateLimit-Remaining", "rate_remaining"), ("X-RateLimit-Reset", "rate_reset")):
                    if header in headers:
                        metadata[field] = headers[header]
                if code == 429:
                    metadata["status"] = "quota_exceeded" if headers.get("X-ELS-Status") == "QUOTA_EXCEEDED" else "rate_limited"
                    metadata["stop_reason"] = "http_429_unclassified" if metadata["status"] == "rate_limited" and not headers.get("X-ELS-Status") else metadata["status"]
                    metadata["next_attempt_at"] = headers.get("X-RateLimit-Reset") if metadata["status"] == "quota_exceeded" else headers.get("Retry-After")
                    break
                if code >= 500:
                    metadata["status"] = "transient_error"
                    break
                if code >= 400:
                    metadata["status"] = "failed"
                    break
                payload = response.json()
                search = payload["search-results"]
                entries = search["entry"]
                total = int(search["opensearch:totalResults"])
                if not isinstance(entries, list) or len(entries) > count or total < 0:
                    raise ValueError("Invalid Scopus page")
            except (requests.Timeout, requests.ConnectionError):
                metadata["status"] = "transient_error"
                break
            except (ValueError, KeyError, TypeError, IndexError):
                metadata["status"] = "failed"
                break
            if not entries:
                metadata["status"] = "completed_exhausted" if offset >= total else "failed"
                break
            page_number = metadata["page_count"] + 1
            raw_path = store.write_raw_page(metadata["batch_id"], page_number, payload)
            metadata["raw_page_files"].append(str(raw_path.relative_to(store.root)))
            metadata["page_count"] = page_number
            metadata["record_count"] += len(entries)
            metadata["next_offset"] += len(entries)
            metadata["total_results"] = total
            metadata["updated_at"] = utc_now()
            store.write_metadata(metadata)
            if metadata["next_offset"] >= total:
                metadata["status"] = "completed_exhausted"
                break
        if metadata["record_count"] >= metadata["target_records"]:
            metadata["status"] = "completed_target"
        metadata["stop_reason"] = metadata.get("stop_reason") if metadata["status"] == "rate_limited" else metadata["status"]
        metadata["updated_at"] = utc_now()
        path = store.write_metadata(metadata)
        return ScopusSummary(metadata["batch_id"], metadata["status"], metadata["record_count"], path)


def run_scopus_years(
    bronze_root: str | Path,
    *,
    query: str,
    first_year: int,
    last_year: int,
    client: ScopusClient | None = None,
    initial_target_records: int = DEFAULT_INITIAL_TARGET,
    target_records: int | None = None,
    **options: Any,
) -> list[ScopusSummary]:
    """Continue disjoint yearly queries, stopping at the first unresolved partition."""
    store = BatchStateStore(Path(bronze_root), "scopus")
    api = client or ScopusClient.from_environment()
    summaries = []
    for partition_query in split_year_range(query, first_year, last_year):
        while True:
            history = [record for record in store.list_metadata() if record.get("source") == "scopus"]
            existing = [record for record in history if record.get("query") == partition_query and record.get("status") in {"completed_target", "completed_exhausted"}]
            in_progress = [record for record in history if record.get("query") == partition_query and record.get("mode") == "initial" and record.get("status") not in {"completed_target", "completed_exhausted", "failed"}]
            last = max(existing, key=lambda record: record.get("created_at", ""), default=None)
            initial_records = sum(int(record.get("record_count", 0)) for record in history if record.get("mode") == "initial")
            remaining_initial = max(0, initial_target_records - initial_records)
            if remaining_initial == 0 and not in_progress:
                return summaries
            if last and (last["status"] == "completed_exhausted" or last.get("next_offset", 0) >= last.get("total_results", float("inf"))):
                if not summaries or summaries[-1].batch_id != last["batch_id"]:
                    summaries.append(ScopusSummary(last["batch_id"], last["status"], last["record_count"], last["_metadata_path"]))
                break
            summary = run_scopus_ingestion(bronze_root, query=partition_query, client=api,
                                           initial_target_records=initial_target_records if in_progress else remaining_initial,
                                           target_records=target_records, **options)
            summaries.append(summary)
            checkpoint = next(record for record in store.list_metadata() if record["batch_id"] == summary.batch_id)
            if summary.status == "completed_exhausted" or (summary.status == "completed_target" and checkpoint.get("next_offset", 0) >= checkpoint.get("total_results", float("inf"))):
                break
            if summary.status != "completed_target":
                return summaries
            updated = store.list_metadata()
            if sum(int(record.get("record_count", 0)) for record in updated if record.get("mode") == "initial") >= initial_target_records:
                return summaries
    return summaries
