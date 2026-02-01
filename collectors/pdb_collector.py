"""Collector for RCSB Protein Data Bank statistics."""

import os
from datetime import datetime, date
import pandas as pd
import requests

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class PDBCollector(BaseCollector):
    """Collector for RCSB Protein Data Bank structure counts."""

    SEARCH_API = "https://search.rcsb.org/rcsbsearch/v2/query"
    STATS_API = "https://data.rcsb.org/rest/v1/holdings/current/entry_ids"

    def __init__(self, data_dir: str = "data/pdb"):
        self.data_dir = data_dir

    @property
    def source_id(self) -> str:
        return "pdb"

    @property
    def source_info(self) -> SourceInfo:
        return SourceInfo(
            id="pdb",
            name="Protein Data Bank",
            description="3D structures of proteins, nucleic acids, and complex assemblies",
            url="https://www.rcsb.org/stats/growth",
            color="#ef4444",
            icon="crystal"
        )

    def collect(self) -> None:
        """Fetch PDB growth statistics using the RCSB Search API."""
        os.makedirs(self.data_dir, exist_ok=True)

        yearly_data = []
        current_year = date.today().year

        print("  Fetching PDB yearly statistics...")

        for year in range(1976, current_year + 1):
            query = {
                "query": {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_accession_info.initial_release_date",
                        "operator": "range",
                        "value": {
                            "from": f"{year}-01-01",
                            "to": f"{year}-12-31",
                            "include_lower": True,
                            "include_upper": True
                        }
                    }
                },
                "return_type": "entry",
                "request_options": {
                    "return_all_hits": False,
                    "results_content_type": ["experimental"],
                    "paginate": {
                        "start": 0,
                        "rows": 0
                    }
                }
            }

            try:
                response = requests.post(self.SEARCH_API, json=query, timeout=30)
                if response.status_code == 200:
                    result = response.json()
                    count = result.get('total_count', 0)
                    yearly_data.append({
                        'year': year,
                        'annual': count
                    })
                    if year % 10 == 0 or year == current_year:
                        print(f"    {year}: {count:,} structures")
                else:
                    print(f"    {year}: API error {response.status_code}")
                    yearly_data.append({'year': year, 'annual': 0})
            except Exception as e:
                print(f"    {year}: Error - {e}")
                yearly_data.append({'year': year, 'annual': 0})

        # Compute cumulative
        running_total = 0
        for entry in yearly_data:
            running_total += entry['annual']
            entry['cumulative'] = running_total

        df = pd.DataFrame(yearly_data)
        df.to_parquet(os.path.join(self.data_dir, "pdb_growth.parquet"))
        print(f"  Total structures: {running_total:,}")

    def transform(self) -> CollectorOutput:
        """Transform PDB data to standard format."""
        df = pd.read_parquet(os.path.join(self.data_dir, "pdb_growth.parquet"))

        # Convert to timeseries (use Jan 1 of each year as date)
        timeseries_data = [
            TimeseriesPoint(
                date=f"{int(row['year'])}-01-01",
                value=int(row['annual']),
                cumulative=int(row['cumulative'])
            )
            for _, row in df.iterrows()
        ]

        current_total = int(df['cumulative'].iloc[-1])

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="structures",
                    name="Protein Structures",
                    unit="structures",
                    current_value=current_total,
                    description="Total 3D macromolecular structures"
                )
            ],
            timeseries=[
                Timeseries(metric_id="structures", data=timeseries_data)
            ],
            update_frequency="weekly",
            data_license="CC0 1.0"
        )
