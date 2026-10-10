"""Cursor-paginated OpenAlex ingestion with recoverable Bronze batch state."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import requests
import time


OPENALEX_WORKS_URL = "https://api.openalex.org/works"
DEFAULT_INITIAL_TARGET = 100_000
DEFAULT_INCREMENTAL_LIMIT = 1_000
DEFAULT_PAGE_SIZE = 200


class IngestionError(RuntimeError):
    """Base error for safe, user-facing ingestion failures."""


class TokenConfigurationError(IngestionError):
    """Raised when the runtime token is absent or blank."""


class OpenAlexRequestError(IngestionError):
    """Raised without copying request URLs that could contain credentials."""

    def __init__(self, message: str, status: str = "transient_error", http_status: int | None = None, retry_after: str | None = None):
        super().__init__(message)
        self.status = status
        self.http_status = http_status
        self.retry_after = retry_after


class BatchStateError(IngestionError):
    """Raised when Bronze state cannot be resumed safely."""


@dataclass(frozen=True)
class IngestionSummary:
    """The safe subset of batch information printed by the scheduler."""

    batch_id: str
    mode: str
    status: str
    pages: int
    records: int
    metadata_path: Path


def utc_now() -> str:
    """Return a sortable UTC timestamp for metadata and audit output."""

    return datetime.now(timezone.utc).isoformat()


class OpenAlexClient:
    """Small API client that keeps the token out of logs and persisted state."""

    def __init__(
        self,
        token: str,
        *,
        session: Any | None = None,
        base_url: str = OPENALEX_WORKS_URL,
        timeout_seconds: int = 60,
        max_retries: int = 2,
        filter_expression: str = "",
    ) -> None:
        if not token or not token.strip():
            raise TokenConfigurationError("OPENALEX_TOKEN must be set")
        self.filter_expression = filter_expression.strip()
        self._token = token.strip()
        self._session = session or requests.Session()
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    @classmethod
    def from_environment(cls) -> "OpenAlexClient":
        """Build a client from the container environment without reading a file."""

        from src.search_criteria import openalex_filter

        configured = os.environ.get("OPENALEX_FILTER")
        if configured is None:
            expression = openalex_filter()
        else:
            expression = "" if configured.strip().lower() == "none" else configured
        return cls(os.environ.get("OPENALEX_TOKEN", ""), filter_expression=expression)

    def _request_json(self, params: dict[str, Any]) -> Any:
        """GET with bounded retries; errors are sanitized so the token never leaks."""

        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.get(
                    self._base_url,
                    params=params,
                    timeout=self._timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                status = "rate_limited" if status_code == 429 else (
                    "failed" if status_code is not None and status_code < 500 else "transient_error"
                )
                retry_after = getattr(getattr(exc, "response", None), "headers", {}).get("Retry-After")
                if status in {"transient_error", "rate_limited"} and attempt < self._max_retries:
                    try:
                        delay = float(retry_after) if retry_after is not None else attempt + 1
                    except ValueError:
                        delay = attempt + 1
                    time.sleep(min(max(delay, 0), 60))
                    continue
                suffix = f" (HTTP {status_code})" if status_code else ""
                raise OpenAlexRequestError(f"OpenAlex request failed{suffix}", status, status_code, retry_after) from None
            except (TypeError, ValueError):
                raise OpenAlexRequestError("OpenAlex returned invalid JSON", "failed") from None
        return payload

    def for_year(self, year: int) -> "OpenAlexClient":
        """Same client restricted to one publication year, with its own filter lineage."""

        import copy

        clone = copy.copy(self)
        clone.filter_expression = ",".join(
            part for part in (self.filter_expression, f"publication_year:{year}") if part
        )
        return clone

    def year_totals(self, first_year: int, last_year: int) -> dict[int, int]:
        """Volume per publication year from a single group_by request."""

        years = f"publication_year:{first_year}-{last_year}"
        params = {
            "api_key": self._token,
            "filter": ",".join(part for part in (self.filter_expression, years) if part),
            "group_by": "publication_year",
        }
        payload = self._request_json(params)
        groups = payload.get("group_by") if isinstance(payload, dict) else None
        if not isinstance(groups, list):
            raise OpenAlexRequestError("OpenAlex response did not contain year groups", "failed")
        counts = {int(group["key"]): int(group["count"]) for group in groups if str(group.get("key", "")).isdigit()}
        return {year: counts.get(year, 0) for year in range(first_year, last_year + 1)}

    def fetch_page(self, *, cursor: str, per_page: int) -> dict[str, Any]:
        """Fetch one cursor page and raise sanitized errors on transport failures."""

        if not cursor:
            raise IngestionError("A non-empty OpenAlex cursor is required")
        if per_page < 1 or per_page > DEFAULT_PAGE_SIZE:
            raise IngestionError("OpenAlex per-page must be between 1 and 200")

        # The token is sent only as an HTTP parameter. It is never included in
        # a batch metadata record, log message, exception text, or notebook.
        params = {
            "api_key": self._token,
            "cursor": cursor,
            "per-page": per_page,
        }
        if self.filter_expression:
            params["filter"] = self.filter_expression
        payload = self._request_json(params)

        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
            raise OpenAlexRequestError("OpenAlex response did not contain a results list", "failed")
        if not isinstance(payload.get("meta"), dict) or "next_cursor" not in payload["meta"]:
            raise OpenAlexRequestError("OpenAlex response did not contain a next cursor", "failed")
        return payload

    def rate_limit(self) -> dict[str, Any]:
        try:
            response = self._session.get(
                "https://api.openalex.org/rate-limit",
                params={"api_key": self._token}, timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            limit = response.json()["rate_limit"]
            return {"credits_remaining": int(limit["credits_remaining"]), "resets_at": limit["resets_at"]}
        except (requests.RequestException, KeyError, TypeError, ValueError):
            raise OpenAlexRequestError("OpenAlex rate-limit check failed") from None


class BatchStateStore:
    """Own the Bronze layout and atomically update per-batch state."""

    def __init__(self, bronze_root: Path, source: str = "openalex") -> None:
        self.root = Path(bronze_root)
        self.metadata_dir = self.root / "batch_metadata"
        self.raw_dir = self.root / source
        self.lock_path = self.root / ".pipeline.lock"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Prevent overlapping scheduler invocations from advancing one batch twice."""

        import fcntl

        self.root.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def list_metadata(self) -> list[dict[str, Any]]:
        records = []
        for path in sorted(self.metadata_dir.glob("*.json")):
            with path.open(encoding="utf-8") as metadata_file:
                record = json.load(metadata_file)
            if not isinstance(record, dict) or not record.get("batch_id"):
                raise BatchStateError(f"Invalid batch metadata: {path.name}")
            record["_metadata_path"] = path
            records.append(record)
        return records

    def write_metadata(self, metadata: dict[str, Any]) -> Path:
        batch_id = metadata.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id:
            raise BatchStateError("Batch metadata requires a batch_id")
        target = self.metadata_dir / f"{batch_id}.json"
        temporary = self.metadata_dir / f".{batch_id}.{uuid.uuid4().hex}.tmp"
        serializable = {key: value for key, value in metadata.items() if not key.startswith("_")}
        temporary.write_text(
            json.dumps(serializable, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, target)
        return target

    def write_raw_page(self, batch_id: str, page_number: int, payload: dict[str, Any]) -> Path:
        """Store one API page as Parquet: one row per record, kept as its original JSON text."""

        import pyarrow as pa
        import pyarrow.parquet as pq

        if isinstance(payload.get("results"), list):
            records = payload["results"]
        else:
            records = payload.get("search-results", {}).get("entry") or []
        batch_dir = self.raw_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        target = batch_dir / f"page-{page_number:06d}.parquet"
        temporary = batch_dir / f".{target.name}.{uuid.uuid4().hex}.tmp"
        table = pa.table(
            {
                "batch_id": pa.array([batch_id] * len(records), pa.string()),
                "page_number": pa.array([page_number] * len(records), pa.int32()),
                "record_index": pa.array(range(len(records)), pa.int32()),
                "record_json": pa.array([json.dumps(record, ensure_ascii=False) for record in records], pa.string()),
            }
        )
        pq.write_table(table, temporary, compression="zstd", compression_level=9)
        os.replace(temporary, target)
        return target


def _new_batch_metadata(
    *,
    batch_id: str,
    mode: str,
    target_records: int,
    start_cursor: str | None,
    filter_expression: str = "",
) -> dict[str, Any]:
    created_at = utc_now()
    return {
        "batch_id": batch_id,
        "source": "openalex",
        "mode": mode,
        "status": "running",
        "target_records": target_records,
        "record_count": 0,
        "page_count": 0,
        "next_cursor": start_cursor,
        "raw_page_dir": str(Path("openalex") / batch_id),
        "raw_page_files": [],
        "filter": filter_expression,
        "created_at": created_at,
        "updated_at": created_at,
    }


def _batch_id(mode: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{mode}-{timestamp}-{uuid.uuid4().hex[:8]}"


def _attempt_deferred(next_attempt_at: Any) -> bool:
    """True while an ISO reset time lies in the future; unparseable values never block."""

    try:
        resume_at = datetime.fromisoformat(str(next_attempt_at).replace("Z", "+00:00"))
    except ValueError:
        return False
    if resume_at.tzinfo is None:
        resume_at = resume_at.replace(tzinfo=timezone.utc)
    return resume_at > datetime.now(timezone.utc)


def _latest_completed(metadata: list[dict[str, Any]]) -> dict[str, Any] | None:
    completed = [record for record in metadata if record.get("status") in {"completed", "completed_target", "completed_exhausted"}]
    if not completed:
        return None
    return max(completed, key=lambda record: record.get("created_at", ""))


def _run_ingestion(
    store: BatchStateStore,
    client: OpenAlexClient,
    *,
    initial_target_records: int,
    incremental_limit: int,
    page_size: int,
    deadline: float | None = None,
) -> IngestionSummary:
    if initial_target_records < 1:
        raise IngestionError("Initial target must be positive")
    if incremental_limit < 1:
        raise IngestionError("Incremental limit must be positive")
    if page_size < 1 or page_size > DEFAULT_PAGE_SIZE:
        raise IngestionError("Page size must be between 1 and 200")

    # State is scoped to the search filter: a cursor from another filter (or from the
    # old unfiltered ingestion) is meaningless for this one.
    active_filter = getattr(client, "filter_expression", "")
    metadata_records = [
        record for record in store.list_metadata()
        if record.get("source", "openalex") == "openalex" and record.get("filter", "") == active_filter
    ]
    running = [record for record in metadata_records if record.get("status") in {"running", "transient_error", "rate_limited", "time_budget_reached"}]
    if len(running) > 1:
        raise BatchStateError("Multiple running batches cannot be resumed safely")

    if running:
        metadata = running[0]
        mode = metadata.get("mode")
        target_records = int(metadata.get("target_records", 0))
        if mode not in {"initial", "incremental"} or target_records < 1:
            raise BatchStateError("Running batch metadata is incomplete")
        if metadata.get("next_attempt_at") and _attempt_deferred(metadata["next_attempt_at"]):
            # Credits have not reset yet: wait without requests or a new metadata file.
            status = "quota_exceeded" if metadata.get("stop_reason") == "insufficient_credits" else metadata["status"]
            return IngestionSummary(metadata["batch_id"], mode, status, int(metadata.get("page_count", 0)), int(metadata.get("record_count", 0)), metadata["_metadata_path"])
    else:
        initial_complete = any(
            record.get("mode") == "initial" and record.get("status") in {"completed", "completed_target", "completed_exhausted"}
            for record in metadata_records
        )
        exhausted = _latest_completed(metadata_records)
        if exhausted and exhausted.get("status") == "completed_exhausted":
            return IngestionSummary(exhausted["batch_id"], exhausted["mode"], "completed_exhausted", exhausted["page_count"], exhausted["record_count"], exhausted["_metadata_path"])
        mode = "incremental" if initial_complete else "initial"
        target_records = incremental_limit if mode == "incremental" else initial_target_records
        previous = _latest_completed(metadata_records) if mode == "incremental" else None
        start_cursor = previous.get("next_cursor") if previous else "*"
        metadata = _new_batch_metadata(
            batch_id=_batch_id(mode),
            mode=mode,
            target_records=target_records,
            start_cursor=start_cursor,
            filter_expression=active_filter,
        )
        store.write_metadata(metadata)

    cursor = metadata.get("next_cursor")
    record_count = int(metadata.get("record_count", 0))
    page_count = int(metadata.get("page_count", 0))
    page_files = list(metadata.get("raw_page_files", []))

    # A completed initial batch is the only switch that permits incremental
    # mode. A running initial batch always resumes with its original cursor.
    if cursor is None:
        metadata["status"] = "completed_exhausted"
        metadata["completed_at"] = utc_now()
        metadata["updated_at"] = utc_now()
        metadata["note"] = "No cursor remained after the previous completed batch"
        metadata_path = store.write_metadata(metadata)
        return IngestionSummary(
            batch_id=metadata["batch_id"],
            mode=mode,
            status="completed_exhausted",
            pages=page_count,
            records=record_count,
            metadata_path=metadata_path,
        )

    # Credits are read once per batch: a page request that runs out of credits is
    # already reported by the API as rate_limited, and checking every page doubled the requests.
    credits_checked = False
    while record_count < target_records:
        if not cursor:
            break
        if deadline is not None and time.monotonic() >= deadline:
            # Pause rather than finish: the batch resumes from its saved cursor next run
            # and only reaches Silver once complete, so a time cut never fragments data.
            metadata.update(status="time_budget_reached", stop_reason="time_budget_reached", updated_at=utc_now())
            path = store.write_metadata(metadata)
            return IngestionSummary(metadata["batch_id"], mode, "time_budget_reached", page_count, record_count, path)

        remaining = target_records - record_count
        request_size = min(page_size, remaining, DEFAULT_PAGE_SIZE)
        try:
            if metadata.get("check_rate_limit", True) and not credits_checked:
                credits_checked = True
                limit = client.rate_limit()
                metadata["credits_remaining"] = limit["credits_remaining"]
                if limit["credits_remaining"] < 10:
                    # Pause, never end: a terminal status would strand this batch's pages
                    # and restart the lineage from an older cursor. The run still reports
                    # quota_exceeded, and the batch resumes once next_attempt_at passes.
                    metadata.update(status="rate_limited", stop_reason="insufficient_credits", next_attempt_at=limit["resets_at"], updated_at=utc_now())
                    path = store.write_metadata(metadata)
                    return IngestionSummary(metadata["batch_id"], mode, "quota_exceeded", page_count, record_count, path)
            payload = client.fetch_page(cursor=cursor, per_page=request_size)
        except OpenAlexRequestError as exc:
            metadata.update(status=exc.status, http_status=exc.http_status, stop_reason=str(exc), updated_at=utc_now())
            if exc.retry_after:
                metadata["next_attempt_at"] = exc.retry_after
            store.write_metadata(metadata)
            raise
        results = payload["results"]
        if len(results) > remaining:
            # The API should honor per-page. Refusing an oversized response is
            # safer than silently violating the incremental 1,000-record cap.
            metadata.update(status="failed", stop_reason="oversized_page", updated_at=utc_now())
            store.write_metadata(metadata)
            raise OpenAlexRequestError("OpenAlex returned more records than requested", "failed")
        next_cursor = (payload.get("meta") or {}).get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            metadata.update(status="failed", stop_reason="invalid_next_cursor", updated_at=utc_now())
            store.write_metadata(metadata)
            raise OpenAlexRequestError("OpenAlex returned an invalid next cursor", "failed")
        if next_cursor == cursor and record_count + len(results) < target_records:
            metadata.update(status="failed", stop_reason="stalled_cursor", updated_at=utc_now())
            store.write_metadata(metadata)
            raise OpenAlexRequestError("OpenAlex cursor did not advance", "failed")
        if not results and next_cursor is not None:
            metadata.update(status="failed", stop_reason="empty_nonterminal_page", updated_at=utc_now())
            store.write_metadata(metadata)
            raise OpenAlexRequestError("OpenAlex returned an empty non-terminal page", "failed")
        if results and next_cursor is None and record_count + len(results) < target_records:
            metadata.update(status="failed", stop_reason="missing_next_cursor", updated_at=utc_now())
            store.write_metadata(metadata)
            raise OpenAlexRequestError("OpenAlex returned a non-terminal page without a cursor", "failed")

        if results:
            page_count += 1
            page_path = store.write_raw_page(metadata["batch_id"], page_count, payload)
            page_files.append(str(page_path.relative_to(store.root)))
            record_count += len(results)
        metadata.update(
            {
                "record_count": record_count,
                "page_count": page_count,
                "next_cursor": next_cursor,
                "raw_page_files": page_files,
                "last_page_record_count": len(results),
                "updated_at": utc_now(),
            }
        )
        store.write_metadata(metadata)
        cursor = next_cursor
        if cursor is None:
            break

    metadata["status"] = "completed_target" if record_count >= target_records else "completed_exhausted"
    metadata["stop_reason"] = metadata["status"]
    metadata["completed_at"] = utc_now()
    metadata["updated_at"] = utc_now()
    metadata_path = store.write_metadata(metadata)
    return IngestionSummary(
        batch_id=metadata["batch_id"],
        mode=mode,
        status=metadata["status"],
        pages=page_count,
        records=record_count,
        metadata_path=metadata_path,
    )


def run_ingestion(
    bronze_root: str | Path,
    *,
    client: OpenAlexClient | None = None,
    initial_target_records: int = DEFAULT_INITIAL_TARGET,
    incremental_limit: int = DEFAULT_INCREMENTAL_LIMIT,
    page_size: int = DEFAULT_PAGE_SIZE,
    deadline: float | None = None,
) -> IngestionSummary:
    """Run one recoverable batch, selecting initial or incremental mode safely."""

    store = BatchStateStore(Path(bronze_root))
    api_client = client or OpenAlexClient.from_environment()
    with store.lock():
        return _run_ingestion(
            store,
            api_client,
            initial_target_records=initial_target_records,
            incremental_limit=incremental_limit,
            page_size=page_size,
            deadline=deadline,
        )


def run_openalex_years(
    bronze_root: str | Path,
    *,
    client: OpenAlexClient | None = None,
    first_year: int,
    last_year: int,
    year_quotas: dict[int, int],
    year_totals: dict[int, int],
    incremental_limit: int = DEFAULT_INCREMENTAL_LIMIT,
    page_size: int = DEFAULT_PAGE_SIZE,
    deadline: float | None = None,
) -> list[IngestionSummary]:
    """Fill each year's initial quota, then grow the least filled year first.

    Every year is its own filter lineage, so its cursor and batches never mix with
    another year's. Initial runs finish all pending quotas; later runs spend one
    incremental budget in order of how complete each year is.
    """

    store = BatchStateStore(Path(bronze_root))
    base = client or OpenAlexClient.from_environment()
    clients = {
        year: base.for_year(year)
        for year in range(first_year, last_year + 1)
        if year_quotas.get(year, 0) > 0
    }

    def history(year: int) -> list[dict[str, Any]]:
        expression = clients[year].filter_expression
        return [
            record for record in store.list_metadata()
            if record.get("source", "openalex") == "openalex" and record.get("filter", "") == expression
        ]

    def initial_done(year: int) -> bool:
        return any(
            record.get("mode") == "initial" and record.get("status") in {"completed", "completed_target", "completed_exhausted"}
            for record in history(year)
        )

    def fill_ratio(year: int) -> float:
        total = year_totals.get(year, 0)
        taken = sum(int(record.get("record_count", 0)) for record in history(year))
        return taken / total if total else 1.0

    summaries: list[IngestionSummary] = []
    pending = [year for year in clients if not initial_done(year)]
    if pending:
        for year in pending:
            summaries.append(run_ingestion(
                bronze_root, client=clients[year], initial_target_records=year_quotas[year],
                incremental_limit=incremental_limit, page_size=page_size, deadline=deadline,
            ))
            if summaries[-1].status == "time_budget_reached":
                break
        return summaries
    budget = incremental_limit
    for year in sorted(clients, key=fill_ratio):
        if budget <= 0:
            break
        summary = run_ingestion(
            bronze_root, client=clients[year], initial_target_records=year_quotas[year],
            incremental_limit=budget, page_size=page_size, deadline=deadline,
        )
        budget -= summary.records
        summaries.append(summary)
        if summary.status == "time_budget_reached":
            break
    return summaries
