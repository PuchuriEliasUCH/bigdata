"""Publish validated Silver compactions through an atomic read manifest."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark_pipeline import ProcessingError, _processing_lock


def _manifest_path(root: Path) -> Path:
    return root / "_compact" / "manifest.json"


def _manifest(root: Path) -> dict:
    path = _manifest_path(root)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def active_silver_paths(silver_root: Path) -> list[Path]:
    """Resolve only the published version of each batch group."""
    root = Path(silver_root)
    groups = _manifest(root)
    replaced = {batch for entry in groups.values() for batch in entry["batches"]}
    paths = sorted(path for path in (root / "works").glob("*") if path.is_dir() and path.name not in replaced)
    paths.extend(root / entry["path"] for entry in groups.values())
    return paths


def resolve_completed_paths(silver_root: Path, completed_paths: list[Path]) -> list[Path]:
    """Use a compacted generation only when all its constituent batches completed."""
    manifest = _manifest(Path(silver_root))
    names = {path.name for path in completed_paths}
    replaced = set()
    compacted = []
    for entry in manifest.values():
        if set(entry["batches"]).issubset(names):
            replaced.update(entry["batches"])
            compacted.append(Path(silver_root) / entry["path"])
    return [path for path in completed_paths if path.name not in replaced] + compacted


def prune_compacted_originals(spark: SparkSession, silver_root: Path, group: str) -> int:
    """Explicitly remove original files only after revalidating the published generation."""
    root = Path(silver_root)
    with _processing_lock(root):
        entry = _manifest(root).get(group)
        if not entry:
            raise ProcessingError("Compaction group is not published")
        published = root / entry["path"]
        if not published.is_dir() or spark.read.parquet(str(published)).count() != entry["rows"]:
            raise ProcessingError("Published compaction failed validation")
        removed = 0
        for batch in entry["batches"]:
            original = root / "works" / batch
            if original.is_dir():
                shutil.rmtree(original)
                removed += 1
        return removed


def compact_batches(
    spark: SparkSession, silver_root: Path, batch_ids: list[str], *, group: str, force: bool = False
) -> Path:
    """Compact complete, schema-compatible batches; preserve old data for rollback."""
    root = Path(silver_root)
    if not batch_ids or any(not value or Path(value).name != value for value in [group, *batch_ids]):
        raise ValueError("A group and safe batch identifiers are required")
    batches = sorted(set(batch_ids))
    with _processing_lock(root):
        manifest = _manifest(root)
        if group in manifest and manifest[group]["batches"] == batches and not force:
            return root / manifest[group]["path"]
        owned = {batch for key, entry in manifest.items() if key != group for batch in entry["batches"]}
        if owned.intersection(batches):
            raise ProcessingError("Batches already belong to another compacted group")
        frames = []
        for batch in batches:
            path = root / "works" / batch
            if not path.is_dir():
                raise ProcessingError(f"Missing Silver batch {batch}")
            frames.append(spark.read.parquet(str(path)))
        schema = frames[0].schema
        if any(frame.schema != schema for frame in frames[1:]):
            raise ProcessingError("Compaction requires matching schemas")
        original_count = sum(frame.count() for frame in frames)
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)
        if "source" in combined.columns and combined.select("source").distinct().count() != 1:
            raise ProcessingError("Compaction group must contain one source")
        # Keep the complete rows: year may be null; do not silently drop them.
        target = root / "_compact" / f"{group}-{uuid.uuid4().hex}"
        combined.coalesce(max(1, (original_count + 99_999) // 100_000)).write.option("compression", "snappy").parquet(str(target))
        published = spark.read.parquet(str(target))
        if published.schema != schema or published.count() != original_count:
            raise ProcessingError("Compaction validation failed")
        manifest[group] = {"batches": batches, "path": str(target.relative_to(root)), "rows": original_count}
        path = _manifest_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".manifest-{uuid.uuid4().hex}.tmp"
        temporary.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)
        return target
