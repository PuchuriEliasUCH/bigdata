"""Recoverable Scopus Search ingestion with bounded result-window pagination."""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
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


FLOOR_LOAD_DATE = date(1960, 1, 1)
# Fixed ceiling: split points must be identical on every run or stored partitions would be refetched.
CEILING_LOAD_DATE = date(2040, 1, 1)
Window = tuple["date | None", "date | None"]


def window_query(base_query: str, low: date | None, high: date | None) -> str:
    """Restrict a query to inclusive load dates; None leaves that side open.

    AFT and BEF are exclusive in Scopus, so the bounds sit one day outside.
    """
    if low is None and high is None:
        return base_query
    clauses = [f"({base_query})"]
    if low is not None:
        clauses.append(f"ORIG-LOAD-DATE AFT {(low - timedelta(days=1)):%Y%m%d}")
    if high is not None:
        clauses.append(f"ORIG-LOAD-DATE BEF {(high + timedelta(days=1)):%Y%m%d}")
    return " AND ".join(clauses)


def split_window(low: date | None, high: date | None, ceiling: date = CEILING_LOAD_DATE) -> list[Window]:
    """Split a load-date window in two disjoint halves that keep its open ends."""
    start = low or FLOOR_LOAD_DATE
    end = high or ceiling
    if start >= end:
        return []
    middle = start + timedelta(days=(end - start).days // 2)
    return [(low, middle), (middle + timedelta(days=1), high)]


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
    split_oversized: bool = False,
    base_query: str | None = None,
) -> ScopusSummary:
    if not query or (target_records is not None and target_records < 1) or initial_target_records < 1 or not 1 <= page_size <= 200 or result_window < 1 or max_retries < 0:
        raise IngestionError("Invalid Scopus query or pagination settings")
    store = BatchStateStore(Path(bronze_root), "scopus")
    api = client or ScopusClient.from_environment()
    with store.lock():
        history = [m for m in store.list_metadata() if m.get("source") == "scopus" and m.get("query") == query]
        candidates = [m for m in history if m.get("status") not in {"completed_target", "completed_exhausted", "failed", "oversized"}]
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
            "raw_page_dir": "", "raw_page_files": [], "base_query": base_query or query, "created_at": utc_now(),
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
                # Scopus reports an empty result set as one error entry, so a zero total
                # means no records and error entries are never stored as records.
                entries = [] if total == 0 else [entry for entry in entries if "error" not in entry]
                search["entry"] = entries
            except (requests.Timeout, requests.ConnectionError):
                metadata["status"] = "transient_error"
                break
            except (ValueError, KeyError, TypeError, IndexError):
                metadata["status"] = "failed"
                break
            if split_oversized and metadata["page_count"] == 0 and total > result_window:
                # The first page already reveals the total, so the caller can split
                # this query without storing records its sub-windows will fetch again.
                metadata["total_results"] = total
                metadata["status"] = "oversized"
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


def scopus_year_totals(client: ScopusClient, query: str, first_year: int, last_year: int) -> dict[int, int]:
    """Volume per publication year, from one single-record request per year."""
    totals = {}
    for year, year_query in zip(range(first_year, last_year + 1), split_year_range(query, first_year, last_year)):
        response = client.fetch_page(year_query, 0, 1)
        if response.status_code >= 400:
            raise IngestionError(f"Scopus year count failed (HTTP {response.status_code})")
        try:
            totals[year] = int(response.json()["search-results"]["opensearch:totalResults"])
        except (ValueError, KeyError, TypeError):
            raise IngestionError("Scopus year count returned an invalid response") from None
    return totals


def run_scopus_years(
    bronze_root: str | Path,
    *,
    query: str,
    first_year: int | None = None,
    last_year: int | None = None,
    client: ScopusClient | None = None,
    initial_target_records: int = DEFAULT_INITIAL_TARGET,
    target_records: int | None = None,
    split_oversized: bool = True,
    ceiling: date = CEILING_LOAD_DATE,
    year_quotas: dict[int, int] | None = None,
    year_totals: dict[int, int] | None = None,
    incremental_limit: int | None = None,
    **options: Any,
) -> list[ScopusSummary]:
    """Ingest a query as disjoint partitions that each fit the result window.

    Partitions are years (when a range is given) and, for any partition above the
    window, recursive load-date halves that cover all history. With year_quotas each
    year fills its own initial quota; afterwards runs spend an incremental budget on
    the least filled year first, so every year grows in proportion to its volume.
    """
    if (first_year is None) != (last_year is None):
        raise ValueError("first_year and last_year must be set together")
    store = BatchStateStore(Path(bronze_root), "scopus")
    api = client or ScopusClient.from_environment()
    if first_year is None:
        years: list[tuple[int | None, str]] = [(None, query)]
    else:
        years = list(zip(range(first_year, last_year + 1), split_year_range(query, first_year, last_year)))
    summaries: list[ScopusSummary] = []
    terminal = {"completed_target", "completed_exhausted", "failed", "oversized", "result_window_reached"}

    def scopus_history() -> list[dict[str, Any]]:
        return [record for record in store.list_metadata() if record.get("source") == "scopus"]

    def scoped(history: list[dict[str, Any]], base_query: str, modes: tuple[str, ...]) -> int:
        """Records already taken for one year, or for everything when there are no quotas."""
        return sum(
            int(record.get("record_count", 0)) for record in history
            if record.get("mode") in modes and (year_quotas is None or record.get("base_query") == base_query)
        )

    def quota_for(year: int | None) -> int:
        return initial_target_records if year_quotas is None else year_quotas.get(year, 0)

    def fill_ratio(year: int | None, base_query: str, history: list[dict[str, Any]]) -> float:
        total = (year_totals or {}).get(year) or 0
        return scoped(history, base_query, ("initial", "incremental")) / total if total else 1.0

    start_history = scopus_history()
    initial_done = all(scoped(start_history, base, ("initial",)) >= quota_for(year) for year, base in years)
    incremental_run = initial_done and not any(
        record.get("mode") == "initial" and record.get("status") not in terminal for record in start_history
    )
    budget = (incremental_limit or target_records or DEFAULT_INCREMENTAL_LIMIT) if incremental_run else None
    if incremental_run and year_quotas is not None:
        years.sort(key=lambda item: fill_ratio(item[0], item[1], start_history))

    def advance(partition_query: str, can_split: bool, base_query: str, quota: int) -> str:
        """Return done, split, quota or stop for one partition."""
        nonlocal budget
        while True:
            history = scopus_history()
            mine = [record for record in history if record.get("query") == partition_query]
            if any(record.get("status") == "oversized" for record in mine):
                return "split"
            if any(record.get("status") == "result_window_reached" for record in mine):
                return "done"
            existing = [record for record in mine if record.get("status") in {"completed_target", "completed_exhausted"}]
            in_progress = [record for record in mine if record.get("mode") == "initial" and record.get("status") not in terminal]
            last = max(existing, key=lambda record: record.get("created_at", ""), default=None)
            if last and (last["status"] == "completed_exhausted" or last.get("next_offset", 0) >= last.get("total_results", float("inf"))):
                if not summaries or summaries[-1].batch_id != last["batch_id"]:
                    summaries.append(ScopusSummary(last["batch_id"], last["status"], last["record_count"], last["_metadata_path"]))
                return "done"
            if budget is not None:
                if budget <= 0:
                    return "stop"
                batch_target, batch_initial = budget, initial_target_records
            else:
                remaining_initial = max(0, quota - scoped(history, base_query, ("initial",)))
                if remaining_initial == 0 and not in_progress:
                    return "stop" if year_quotas is None else "quota"
                batch_target = target_records
                batch_initial = quota if in_progress else remaining_initial
            summary = run_scopus_ingestion(bronze_root, query=partition_query, client=api,
                                           initial_target_records=batch_initial, target_records=batch_target,
                                           split_oversized=can_split, base_query=base_query, **options)
            if summary.status == "oversized":
                return "split"
            summaries.append(summary)
            if budget is not None:
                budget -= summary.records
            if summary.status == "result_window_reached":
                return "done"
            checkpoint = next(record for record in store.list_metadata() if record["batch_id"] == summary.batch_id)
            if summary.status == "completed_exhausted" or (summary.status == "completed_target" and checkpoint.get("next_offset", 0) >= checkpoint.get("total_results", float("inf"))):
                return "done"
            if summary.status != "completed_target":
                return "stop"
            if budget is None and scoped(scopus_history(), base_query, ("initial",)) >= quota:
                return "stop" if year_quotas is None else "quota"

    for year, base_query in years:
        pending: list[Window] = [(None, None)]
        while pending:
            low, high = pending.pop()
            halves = split_window(low, high, ceiling) if split_oversized else []
            outcome = advance(window_query(base_query, low, high), bool(halves), base_query, quota_for(year))
            if outcome == "stop":
                return summaries
            if outcome == "quota":
                break
            if outcome == "split":
                pending.extend(reversed(halves))
    return summaries
