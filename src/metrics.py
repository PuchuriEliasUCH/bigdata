"""Reproducible per-batch Bronze/Silver measurements."""

import argparse
import json
from pathlib import Path


def measure_batches(bronze_root: Path, silver_root: Path) -> list[dict]:
    rows = []
    for work in sorted((silver_root / "works").glob("*")):
        if not work.is_dir():
            continue
        batch = work.name
        metadata_path = bronze_root / "batch_metadata" / f"{batch}.json"
        state_path = silver_root / "_processing" / f"{batch}.json"
        if not metadata_path.exists() or not state_path.exists():
            continue
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        files = list(work.glob("part-*.parquet"))
        rows.append({
            "batch_id": batch,
            "source": metadata.get("source", "openalex"),
            "mode": metadata.get("mode", "unknown"),
            "valid_rows": state.get("valid_rows"),
            "rejected_rows": state.get("rejected_rows"),
            "parquet_files": len(files),
            "parquet_bytes": sum(path.stat().st_size for path in files),
            "write_seconds": state.get("write_seconds"),
        })
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, required=True)
    parser.add_argument("--silver", type=Path, required=True)
    args = parser.parse_args()
    for row in measure_batches(args.bronze, args.silver):
        print(json.dumps(row, sort_keys=True))
