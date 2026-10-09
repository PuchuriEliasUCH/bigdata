"""Publish validated Silver compactions through an atomic read manifest."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark_pipeline import PARQUET_WRITE_OPTIONS, ProcessingError, _completed_metadata, _processing_lock


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


def active_rejected_paths(silver_root: Path) -> list[Path]:
    """Rejected rows to read: compacted groups plus batches not yet compacted."""
    root = Path(silver_root)
    groups = {name: entry for name, entry in _manifest(root).items() if entry.get("rejected_path")}
    replaced = {batch for entry in groups.values() for batch in entry["batches"]}
    paths = sorted(path for path in (root / "rejected").glob("*") if path.is_dir() and path.name not in replaced)
    paths.extend(root / entry["rejected_path"] for entry in groups.values())
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
        if entry.get("rejected_path"):
            rejected = root / entry["rejected_path"]
            if not rejected.is_dir() or spark.read.parquet(str(rejected)).count() != entry["rejected_rows"]:
                raise ProcessingError("Published rejected compaction failed validation")
        removed = 0
        for batch in entry["batches"]:
            original = root / "works" / batch
            if original.is_dir():
                shutil.rmtree(original)
                removed += 1
            if entry.get("rejected_path"):
                shutil.rmtree(root / "rejected" / batch, ignore_errors=True)
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
        combined.coalesce(max(1, (original_count + 99_999) // 100_000)).write.options(**PARQUET_WRITE_OPTIONS).parquet(str(target))
        published = spark.read.parquet(str(target))
        if published.schema != schema or published.count() != original_count:
            raise ProcessingError("Compaction validation failed")
        entry = {"batches": batches, "path": str(target.relative_to(root)), "rows": original_count}
        rejected_dirs = [root / "rejected" / batch for batch in batches if (root / "rejected" / batch).is_dir()]
        if rejected_dirs:
            rejected_frames = [spark.read.parquet(str(path)) for path in rejected_dirs]
            rejected = rejected_frames[0]
            for frame in rejected_frames[1:]:
                rejected = rejected.unionByName(frame)
            rejected_count = sum(frame.count() for frame in rejected_frames)
            rejected_target = root / "_compact" / f"{group}-rejected-{uuid.uuid4().hex}"
            rejected.coalesce(1).write.options(**PARQUET_WRITE_OPTIONS).parquet(str(rejected_target))
            if spark.read.parquet(str(rejected_target)).count() != rejected_count:
                raise ProcessingError("Rejected compaction validation failed")
            entry.update(rejected_path=str(rejected_target.relative_to(root)), rejected_rows=rejected_count)
        manifest[group] = entry
        path = _manifest_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".manifest-{uuid.uuid4().hex}.tmp"
        temporary.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)
        return target


@dataclass(frozen=True)
class CompactionSummary:
    """Groups published this run and groups skipped because they could not be compacted."""

    groups: tuple[str, ...]
    skipped: tuple[str, ...]


def _group_key(metadata: dict) -> tuple[str, str]:
    """Source and publication year of a batch, from its ingestion lineage."""
    text = metadata.get("filter") or metadata.get("base_query") or metadata.get("query") or ""
    match = re.search(r"(?:publication_year:|PUBYEAR = )(\d{4})", text)
    return metadata.get("source", "openalex"), match.group(1) if match else "unknown"


def compact_pending(
    spark: SparkSession, bronze_root: Path, silver_root: Path, *, min_batches: int = 10
) -> CompactionSummary:
    """Compact uncompacted Silver batches per source and year once enough have piled up.

    Every run publishes a new immutable group (openalex-2024-0001, -0002, ...), so
    nothing already compacted is rewritten and no old generation is left behind.
    """
    root = Path(silver_root)
    owned = {batch for entry in _manifest(root).values() for batch in entry["batches"]}
    lineage = {record["batch_id"]: _group_key(record) for record in _completed_metadata(Path(bronze_root))}
    pending: dict[tuple[str, str], list[str]] = defaultdict(list)
    for work in sorted((root / "works").glob("*")):
        state_path = root / "_processing" / f"{work.name}.json"
        if not work.is_dir() or work.name in owned or work.name not in lineage or not state_path.exists():
            continue
        if json.loads(state_path.read_text(encoding="utf-8")).get("status") == "completed":
            pending[lineage[work.name]].append(work.name)
    created: list[str] = []
    skipped: list[str] = []
    for (source, year), batches in sorted(pending.items()):
        if len(batches) < min_batches:
            continue
        sequence = 1 + sum(1 for name in _manifest(root) if name.startswith(f"{source}-{year}-"))
        group = f"{source}-{year}-{sequence:04d}"
        try:
            compact_batches(spark, root, batches, group=group)
            prune_compacted_originals(spark, root, group)
        except ProcessingError:
            skipped.append(f"{source}-{year}")
            continue
        created.append(group)
    return CompactionSummary(tuple(created), tuple(skipped))
