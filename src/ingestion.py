"""Year-stratified ingestion for both sources: plan quotas once, then run each source."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from src.openalex_ingest import (
    DEFAULT_PAGE_SIZE,
    IngestionSummary,
    OpenAlexClient,
    run_openalex_years,
)
from src.quotas import ensure_year_plan
from src.scopus_ingest import ScopusClient, ScopusSummary, run_scopus_years, scopus_year_totals


def ingest_openalex(
    bronze_root: str | Path,
    *,
    first_year: int,
    last_year: int,
    initial_target: int,
    incremental_limit: int,
    page_size: int = DEFAULT_PAGE_SIZE,
    time_budget: float | None = None,
) -> list[IngestionSummary]:
    client = OpenAlexClient.from_environment()
    key = {"source": "openalex", "filter": client.filter_expression, "first_year": first_year,
           "last_year": last_year, "initial_target": initial_target}
    totals, quotas = ensure_year_plan(
        Path(bronze_root) / "plans" / "openalex.json", key,
        lambda: client.year_totals(first_year, last_year), initial_target,
    )
    return run_openalex_years(
        bronze_root, client=client, first_year=first_year, last_year=last_year,
        year_quotas=quotas, year_totals=totals, incremental_limit=incremental_limit, page_size=page_size,
        deadline=time.monotonic() + time_budget if time_budget else None,
    )


def ingest_scopus(
    bronze_root: str | Path,
    *,
    query: str,
    first_year: int,
    last_year: int,
    initial_target: int,
    incremental_limit: int,
    time_budget: float | None = None,
    **options: Any,
) -> list[ScopusSummary]:
    client = ScopusClient.from_environment()
    key = {"source": "scopus", "query": query, "first_year": first_year,
           "last_year": last_year, "initial_target": initial_target}
    totals, quotas = ensure_year_plan(
        Path(bronze_root) / "plans" / "scopus.json", key,
        lambda: scopus_year_totals(client, query, first_year, last_year), initial_target,
    )
    return run_scopus_years(
        bronze_root, query=query, first_year=first_year, last_year=last_year, client=client,
        initial_target_records=initial_target, incremental_limit=incremental_limit,
        year_quotas=quotas, year_totals=totals,
        deadline=time.monotonic() + time_budget if time_budget else None, **options,
    )
