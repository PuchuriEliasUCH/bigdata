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


OPENALEX_WORKS_URL = "https://api.openalex.org/works"
DEFAULT_INITIAL_TARGET = 200_000
DEFAULT_INCREMENTAL_LIMIT = 1_000
DEFAULT_PAGE_SIZE = 200


class IngestionError(RuntimeError):
    """Base error for safe, user-facing ingestion failures."""


class TokenConfigurationError(IngestionError):
    """Raised when the runtime token is absent or blank."""


class OpenAlexRequestError(IngestionError):
    """Raised without copying request URLs that could contain credentials."""


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
    ) -> None:
        if not token or not token.strip():
            raise TokenConfigurationError("OPENALEX_TOKEN must be set")
        self._token = token.strip()
        self._session = session or requests.Session()
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds

    @classmethod
    def from_environment(cls) -> "OpenAlexClient":
        """Build a client from the container environment without reading a file."""

        return cls(os.environ.get("OPENALEX_TOKEN", ""))

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
        try:
            response = self._session.get(
                self._base_url,
                params=params,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            suffix = f" (HTTP {status_code})" if status_code else ""
            raise OpenAlexRequestError(f"OpenAlex request failed{suffix}") from None
        except (TypeError, ValueError) as exc:
            raise OpenAlexRequestError("OpenAlex returned invalid JSON") from exc

        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
            raise OpenAlexRequestError("OpenAlex response did not contain a results list")
        return payload


class BatchStateStore:
    """Own the Bronze layout and atomically update per-batch state."""

    def __init__(self, bronze_root: Path) -> None:
        self.root = Path(bronze_root)
        self.metadata_dir = self.root / "batch_metadata"
        self.raw_dir = self.root / "openalex"
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
        batch_dir = self.raw_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        target = batch_dir / f"page-{page_number:06d}.json"
        temporary = batch_dir / f".{target.name}.{uuid.uuid4().hex}.tmp"
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )
        os.replace(temporary, target)
        return target


def _new_batch_metadata(
    *,
    batch_id: str,
    mode: str,
    target_records: int,
    start_cursor: str | None,
) -> dict[str, Any]:
    created_at = utc_now()
    return {
        "batch_id": batch_id,
        "mode": mode,
        "status": "running",
        "target_records": target_records,
        "record_count": 0,
        "page_count": 0,
        "next_cursor": start_cursor,
        "raw_page_dir": str(Path("openalex") / batch_id),
        "raw_page_files": [],
        "created_at": created_at,
        "updated_at": created_at,
    }


def _batch_id(mode: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{mode}-{timestamp}-{uuid.uuid4().hex[:8]}"


def _latest_completed(metadata: list[dict[str, Any]]) -> dict[str, Any] | None:
    completed = [record for record in metadata if record.get("status") == "completed"]
    if not completed:
        return None
    return max(completed, key=lambda record: record.get("created_at", ""))


def _mark_failed_initial(
    store: BatchStateStore,
    metadata: dict[str, Any],
    message: str,
) -> None:
    metadata["status"] = "failed"
    metadata["error"] = message
    metadata["updated_at"] = utc_now()
    store.write_metadata(metadata)


def _run_ingestion(
    store: BatchStateStore,
    client: OpenAlexClient,
    *,
    initial_target_records: int,
    incremental_limit: int,
    page_size: int,
) -> IngestionSummary:
    if initial_target_records < 1:
        raise IngestionError("Initial target must be positive")
    if incremental_limit < 1:
        raise IngestionError("Incremental limit must be positive")
    if page_size < 1 or page_size > DEFAULT_PAGE_SIZE:
        raise IngestionError("Page size must be between 1 and 200")

    metadata_records = store.list_metadata()
    running = [record for record in metadata_records if record.get("status") == "running"]
    if len(running) > 1:
        raise BatchStateError("Multiple running batches cannot be resumed safely")

    if running:
        metadata = running[0]
        mode = metadata.get("mode")
        target_records = int(metadata.get("target_records", 0))
        if mode not in {"initial", "incremental"} or target_records < 1:
            raise BatchStateError("Running batch metadata is incomplete")
    else:
        initial_complete = any(
            record.get("mode") == "initial" and record.get("status") == "completed"
            for record in metadata_records
        )
        mode = "incremental" if initial_complete else "initial"
        target_records = incremental_limit if mode == "incremental" else initial_target_records
        previous = _latest_completed(metadata_records) if mode == "incremental" else None
        start_cursor = previous.get("next_cursor") if previous else "*"
        metadata = _new_batch_metadata(
            batch_id=_batch_id(mode),
            mode=mode,
            target_records=target_records,
            start_cursor=start_cursor,
        )
        store.write_metadata(metadata)

    cursor = metadata.get("next_cursor")
    record_count = int(metadata.get("record_count", 0))
    page_count = int(metadata.get("page_count", 0))
    page_files = list(metadata.get("raw_page_files", []))

    # A completed initial batch is the only switch that permits incremental
    # mode. A running initial batch always resumes with its original cursor.
    if mode == "incremental" and cursor is None:
        metadata["status"] = "completed"
        metadata["completed_at"] = utc_now()
        metadata["updated_at"] = utc_now()
        metadata["note"] = "No cursor remained after the previous completed batch"
        metadata_path = store.write_metadata(metadata)
        return IngestionSummary(
            batch_id=metadata["batch_id"],
            mode=mode,
            status="completed",
            pages=page_count,
            records=record_count,
            metadata_path=metadata_path,
        )

    while record_count < target_records:
        if not cursor:
            message = "OpenAlex ended cursor pagination before the target was reached"
            if mode == "initial":
                _mark_failed_initial(store, metadata, message)
            raise IngestionError(message)

        remaining = target_records - record_count
        request_size = min(page_size, remaining, DEFAULT_PAGE_SIZE)
        payload = client.fetch_page(cursor=cursor, per_page=request_size)
        results = payload["results"]
        if len(results) > remaining:
            # The API should honor per-page. Refusing an oversized response is
            # safer than silently violating the incremental 1,000-record cap.
            raise IngestionError("OpenAlex returned more records than requested")
        if not results:
            message = "OpenAlex returned no records before the target was reached"
            if mode == "initial":
                _mark_failed_initial(store, metadata, message)
            raise IngestionError(message)

        next_cursor = (payload.get("meta") or {}).get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise IngestionError("OpenAlex returned an invalid next cursor")
        if next_cursor == cursor and record_count + len(results) < target_records:
            raise IngestionError("OpenAlex cursor did not advance")

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

    metadata["status"] = "completed"
    metadata["completed_at"] = utc_now()
    metadata["updated_at"] = utc_now()
    metadata_path = store.write_metadata(metadata)
    return IngestionSummary(
        batch_id=metadata["batch_id"],
        mode=mode,
        status="completed",
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
        )
