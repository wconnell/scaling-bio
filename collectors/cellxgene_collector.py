"""Collector for CZI CellxGene Census data."""

import os
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import requests

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class CellxGeneCollector(BaseCollector):
    """Collector for CZI CellxGene Census single-cell data.

    Uses only dataset-level metadata (no full cell download required).
    Cell counts come from dataset metadata, dates from CrossRef API.
    """

    def __init__(self, data_dir: str = "data/cellxgene"):
        self.data_dir = data_dir
        self.census_version = "stable"

    @property
    def source_id(self) -> str:
        return "cellxgene"

    @property
    def source_info(self) -> SourceInfo:
        return SourceInfo(
            id="cellxgene",
            name="CellxGene Census",
            description="CZI's single-cell RNA-seq data repository",
            url="https://chanzuckerberg.github.io/cellxgene-census/",
            color="#10b981",
            icon="cell"
        )

    def collect(self) -> None:
        """Fetch dataset metadata from CellxGene Census API (lightweight)."""
        import cellxgene_census

        os.makedirs(self.data_dir, exist_ok=True)

        print("  Opening CellxGene Census...")
        census = cellxgene_census.open_soma(census_version=self.census_version)

        try:
            # Get summary info
            info = census['census_info']['summary'].read().concat().to_pandas()
            info.to_parquet(os.path.join(self.data_dir, "summary.parquet"))

            total_cells = info[info['label'] == 'total_cell_count']['value'].values[0]
            print(f"    Total cells in census: {int(total_cells):,}")

            # Get dataset metadata (includes cell counts per dataset)
            print("  Fetching dataset metadata...")
            ds_meta = census['census_info']['datasets'].read().concat().to_pandas()
            ds_meta.to_parquet(os.path.join(self.data_dir, "datasets.parquet"))
            print(f"    Datasets: {len(ds_meta)}")

        finally:
            census.close()

    def _get_cache_path(self) -> str:
        """Get path to DOI cache file (stored in collectors/ to be tracked by git)."""
        return os.path.join(os.path.dirname(__file__), "cellxgene_doi_cache.json")

    def _load_doi_cache(self) -> dict:
        """Load cached DOI->date mappings."""
        cache_path = self._get_cache_path()
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                return json.load(f)
        return {}

    def _save_doi_cache(self, cache: dict) -> None:
        """Save DOI->date mappings to cache."""
        cache_path = self._get_cache_path()
        with open(cache_path, 'w') as f:
            json.dump(cache, f, indent=2, sort_keys=True)

    def _fetch_single_doi(self, doi: str, max_retries: int = 3) -> tuple:
        """Fetch publication date for a single DOI with retries."""
        if pd.isna(doi):
            return (doi, None)

        for attempt in range(max_retries):
            try:
                url = f"https://api.crossref.org/works/{doi}"
                response = requests.get(url, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    date = data['message'].get('created', {}).get('date-time')
                    return (doi, date)
                elif response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:  # DOI not found
                    return (doi, None)
            except requests.exceptions.Timeout:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            except Exception:
                pass

        return (doi, None)

    def _get_publication_dates(self, dois: list) -> dict:
        """Fetch publication dates from CrossRef API with caching and retries."""
        # Load existing cache
        cache = self._load_doi_cache()
        cached_count = 0
        fetch_count = 0

        # Separate cached vs uncached DOIs
        results = {}
        dois_to_fetch = []

        for doi in dois:
            if doi in cache:
                results[doi] = cache[doi]
                cached_count += 1
            else:
                dois_to_fetch.append(doi)

        print(f"  CrossRef: {cached_count} cached, {len(dois_to_fetch)} to fetch...")

        if dois_to_fetch:
            # Fetch uncached DOIs with thread pool (limited concurrency to avoid rate limits)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self._fetch_single_doi, doi): doi for doi in dois_to_fetch}

                for future in as_completed(futures):
                    doi, date = future.result()
                    results[doi] = date
                    cache[doi] = date  # Cache both successes and failures
                    fetch_count += 1

                    if fetch_count % 50 == 0:
                        print(f"    Fetched {fetch_count}/{len(dois_to_fetch)}...")

            # Save updated cache
            self._save_doi_cache(cache)
            print(f"    Fetched {fetch_count} DOIs, cache updated")

        # Count successful dates
        success_count = sum(1 for d in results.values() if d is not None)
        print(f"  Total DOIs with dates: {success_count}/{len(dois)}")

        return results

    def transform(self) -> CollectorOutput:
        """Transform collected data to standard format."""
        # Load census summary for official totals
        summary = pd.read_parquet(os.path.join(self.data_dir, "summary.parquet"))
        official_total = int(summary[summary['label'] == 'unique_cell_count']['value'].values[0])

        # Load dataset metadata
        ds_meta = pd.read_parquet(os.path.join(self.data_dir, "datasets.parquet"))

        # Dataset metadata includes cell_count column
        if 'cell_count' not in ds_meta.columns:
            # Fallback: some versions might have different column names
            for col in ['dataset_total_cell_count', 'total_cell_count']:
                if col in ds_meta.columns:
                    ds_meta['cell_count'] = ds_meta[col]
                    break

        # Get publication dates from CrossRef
        unique_dois = ds_meta['collection_doi'].dropna().unique().tolist()
        pub_dates = self._get_publication_dates(unique_dois)
        ds_meta['pub_date'] = ds_meta['collection_doi'].map(pub_dates)
        ds_meta['pub_date'] = pd.to_datetime(ds_meta['pub_date'], errors='coerce')
        ds_meta_with_dates = ds_meta.dropna(subset=['pub_date'])

        # Make timezone naive
        ds_meta_with_dates['pub_date'] = ds_meta_with_dates['pub_date'].dt.tz_localize(None)

        # Sort by publication date and compute cumulative
        ds_meta_with_dates = ds_meta_with_dates.sort_values('pub_date')
        ds_meta_with_dates['cum_cells'] = ds_meta_with_dates['cell_count'].cumsum()

        # Aggregate by month for cleaner timeseries
        ds_meta_with_dates['month'] = ds_meta_with_dates['pub_date'].dt.to_period('M')
        monthly = ds_meta_with_dates.groupby('month').agg({
            'cell_count': 'sum',
            'cum_cells': 'last',
        }).reset_index()
        monthly['date'] = monthly['month'].dt.to_timestamp().dt.strftime('%Y-%m-%d')

        # Scale timeseries to match official total (some datasets lack DOIs)
        timeseries_total = int(monthly['cum_cells'].iloc[-1]) if len(monthly) > 0 else 1
        scale_factor = official_total / timeseries_total

        # Build timeseries
        cells_ts = [
            TimeseriesPoint(
                date=row['date'],
                value=int(row['cell_count'] * scale_factor),
                cumulative=int(row['cum_cells'] * scale_factor)
            )
            for _, row in monthly.iterrows()
        ]

        total_cells = official_total

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="cells",
                    name="Single Cells",
                    unit="cells",
                    current_value=total_cells,
                    description="Total single cells profiled"
                )
            ],
            timeseries=[
                Timeseries(metric_id="cells", data=cells_ts)
            ],
            update_frequency="quarterly",
            data_license="CC BY 4.0"
        )
