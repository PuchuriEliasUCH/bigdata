"""End paused Bronze batches for good once ingestion is frozen.

Silver only reads batches whose status is terminal for their source, so a paused
(resumable) batch keeps its pages out of Silver until an ingester finishes it. When no
further ingestion will run, this module rewrites those batches to status "frozen",
which Silver accepts and neither ingester resumes. A later ingestion starts new batches.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from src.openalex_ingest import RESUMABLE_STATUSES as OPENALEX_RESUMABLE_STATUSES
from src.openalex_ingest import BatchStateStore, utc_now
from src.scopus_ingest import RESUMABLE_STATUSES as SCOPUS_RESUMABLE_STATUSES


FROZEN_STATUS = "frozen"
RESUMABLE_STATUSES = {"openalex": OPENALEX_RESUMABLE_STATUSES, "scopus": SCOPUS_RESUMABLE_STATUSES}


def freeze_paused_batches(bronze_root: Path) -> list[str]:
    """Mark every resumable batch that holds pages as frozen; return the frozen batch ids.

    The original status is kept in ``frozen_from``. Batches without pages, terminal
    batches and already frozen batches are left untouched, so the call is idempotent.
    """

    bronze_root = Path(bronze_root)
    if not (bronze_root / "batch_metadata").is_dir():
        return []
    store = BatchStateStore(bronze_root)
    frozen = []
    with store.lock():
        for record in store.list_metadata():
            resumable = RESUMABLE_STATUSES.get(record.get("source"), frozenset())
            if record.get("status") not in resumable or int(record.get("page_count", 0)) <= 0:
                continue
            record.update(status=FROZEN_STATUS, frozen_from=record["status"], frozen_at=utc_now())
            store.write_metadata(record)
            frozen.append(str(record["batch_id"]))
    return sorted(frozen)


def main() -> None:
    bronze_root = Path(os.environ.get("BRONZE_PATH", "/data/bronze"))
    frozen = freeze_paused_batches(bronze_root)
    print(json.dumps({"bronze_root": str(bronze_root), "frozen_batches": len(frozen), "batch_ids": frozen}, indent=2))


if __name__ == "__main__":
    main()
