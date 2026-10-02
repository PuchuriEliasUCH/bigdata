"""Year quotas for stratified ingestion, persisted so reruns never drift."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Callable


def proportional_quotas(totals: dict[int, int], target: int) -> dict[int, int]:
    """Split target across years in proportion to their volume (largest remainder).

    A year never gets more than its own total, so a target above the combined
    volume simply takes everything.
    """
    grand_total = sum(max(0, value) for value in totals.values())
    if target <= 0 or grand_total == 0:
        return {year: 0 for year in totals}
    if target >= grand_total:
        return {year: max(0, value) for year, value in totals.items()}
    shares = {year: target * max(0, value) / grand_total for year, value in totals.items()}
    quotas = {year: int(share) for year, share in shares.items()}
    leftover = target - sum(quotas.values())
    by_remainder = sorted(shares, key=lambda year: (shares[year] - quotas[year], -year), reverse=True)
    for year in by_remainder[:leftover]:
        quotas[year] += 1
    return quotas


def load_plan(path: Path, key: dict[str, Any]) -> dict[str, Any] | None:
    """Return the saved plan only when it was made for exactly this key."""
    if not path.exists():
        return None
    saved = json.loads(path.read_text(encoding="utf-8"))
    return saved["plan"] if saved.get("key") == key else None


def save_plan(path: Path, key: dict[str, Any], plan: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(json.dumps({"key": key, "plan": plan}, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def ensure_year_plan(
    path: Path, key: dict[str, Any], compute_totals: Callable[[], dict[int, int]], target: int
) -> tuple[dict[int, int], dict[int, int]]:
    """Return (totals, quotas) per year, computing and saving them only once per key."""
    plan = load_plan(path, key)
    if plan is None:
        totals = compute_totals()
        plan = {
            "totals": {str(year): value for year, value in totals.items()},
            "quotas": {str(year): value for year, value in proportional_quotas(totals, target).items()},
        }
        save_plan(path, key, plan)
    return (
        {int(year): value for year, value in plan["totals"].items()},
        {int(year): value for year, value in plan["quotas"].items()},
    )
