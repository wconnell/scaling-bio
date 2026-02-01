"""Collector for NCBI Sequence Read Archive data using BigQuery."""

import os
from datetime import datetime
import pandas as pd

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class SRACollector(BaseCollector):
    """Collector for NCBI Sequence Read Archive total bases.

    Uses Google BigQuery public dataset: nih-sra-datastore.sra.metadata
    """

    def __init__(self, data_dir: str = "data/sra"):
        self.data_dir = data_dir

    @property
    def source_id(self) -> str:
        return "sra"

    @property
    def source_info(self) -> SourceInfo:
        return SourceInfo(
            id="sra",
            name="Sequence Read Archive",
            description="NCBI's archive of high-throughput sequencing data",
            url="https://www.ncbi.nlm.nih.gov/sra/docs/sragrowth/",
            color="#2563eb",
            icon="microbe"
        )

    def collect(self) -> None:
        """Fetch SRA total bases by year from BigQuery."""
        from google.cloud import bigquery

        os.makedirs(self.data_dir, exist_ok=True)

        print("  Querying BigQuery for SRA total bases by year...")

        client = bigquery.Client()

        # Note: mbases = megabases, multiply by 1e6 to get actual bases
        # releasedate is a TIMESTAMP column
        query = """
        SELECT
            EXTRACT(YEAR FROM releasedate) as year,
            SUM(mbases) * 1000000 as total_bases,
            COUNT(*) as run_count
        FROM `nih-sra-datastore.sra.metadata`
        WHERE releasedate IS NOT NULL
            AND mbases IS NOT NULL
            AND mbases > 0
        GROUP BY year
        ORDER BY year
        """

        query_job = client.query(query)
        results = query_job.result()

        yearly_data = []
        for row in results:
            if row.year and row.total_bases:
                yearly_data.append({
                    'year': int(row.year),
                    'bases': int(row.total_bases),
                    'runs': int(row.run_count)
                })
                print(f"    {row.year}: {row.total_bases / 1e15:.2f} PB ({row.run_count:,} runs)")

        # Compute cumulative
        running_total = 0
        for entry in yearly_data:
            running_total += entry['bases']
            entry['cumulative_bases'] = running_total

        df = pd.DataFrame(yearly_data)
        df.to_parquet(os.path.join(self.data_dir, "sra_bases.parquet"))
        print(f"  Total bases: {running_total / 1e15:.2f} PB")

    def transform(self) -> CollectorOutput:
        """Transform BigQuery data to standard format."""
        df = pd.read_parquet(os.path.join(self.data_dir, "sra_bases.parquet"))

        # Convert to timeseries (use Jan 1 of each year as date)
        timeseries_data = [
            TimeseriesPoint(
                date=f"{int(row['year'])}-01-01",
                value=int(row['bases']),
                cumulative=int(row['cumulative_bases'])
            )
            for _, row in df.iterrows()
        ]

        current_total = int(df['cumulative_bases'].iloc[-1])

        # Format as petabases
        petabases = current_total / 1e15
        formatted = f"{petabases:.1f} PB"

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="bases",
                    name="Total Bases",
                    unit="bases",
                    current_value=current_total,
                    formatted_value=formatted,
                    description="Total sequenced bases in SRA"
                )
            ],
            timeseries=[
                Timeseries(metric_id="bases", data=timeseries_data)
            ],
            update_frequency="weekly",
            data_license="Public Domain"
        )
