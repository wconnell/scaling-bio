"""Collector for NCBI Sequence Read Archive data."""

import os
import glob
from datetime import datetime, timedelta
import pandas as pd

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class SRACollector(BaseCollector):
    """Collector for NCBI Sequence Read Archive data."""

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
            url="https://www.ncbi.nlm.nih.gov/sra",
            color="#2563eb",
            icon="dna"
        )

    def collect(self) -> None:
        """
        Collect SRA metadata using pysradb.
        This is a simplified version - for full historical data,
        run the original sra_metadata.py script.
        """
        from pysradb.search import SraSearch

        os.makedirs(self.data_dir, exist_ok=True)

        # Check what we already have
        existing_files = glob.glob(os.path.join(self.data_dir, "*.parquet"))

        if existing_files:
            # Get most recent date from existing files
            dates = []
            for f in existing_files:
                basename = os.path.basename(f)
                try:
                    date_str = basename.split('_')[-1].split('.')[0]
                    date = datetime.strptime(date_str, "%Y%m%d")
                    dates.append(date)
                except:
                    continue
            start_date = max(dates) + timedelta(days=1) if dates else datetime(2007, 1, 1)
        else:
            start_date = datetime(2007, 1, 1)

        end_date = datetime.now()

        # Collect in 2-week chunks
        current_date = start_date
        while current_date < end_date:
            next_date = min(current_date + timedelta(days=14), end_date)

            start_str = current_date.strftime("%d-%m-%Y")
            end_str = next_date.strftime("%d-%m-%Y")
            publication_date = f"{start_str}:{end_str}"

            try:
                print(f"  Fetching SRA data for {publication_date}...")
                instance = SraSearch(
                    query='(genomic[Source])',
                    publication_date=publication_date,
                    verbosity=0,
                    return_max=10000000
                )
                instance.search()
                df = instance.get_df()

                if not df.empty:
                    filename = f"sra_search_{current_date.strftime('%Y%m%d')}_to_{next_date.strftime('%Y%m%d')}.parquet"
                    filepath = os.path.join(self.data_dir, filename)
                    df.to_parquet(filepath, index=False)
                    print(f"    Saved {len(df)} records")

            except Exception as e:
                print(f"    Error: {e}")

            current_date = next_date

    def transform(self) -> CollectorOutput:
        """Transform parquet files to standard format."""
        parquet_files = glob.glob(os.path.join(self.data_dir, "*.parquet"))

        if not parquet_files:
            raise ValueError(f"No parquet files found in {self.data_dir}. Run collect() first.")

        # Load and combine all files
        dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not read {f}: {e}")

        if not dfs:
            raise ValueError("Could not read any parquet files")

        combined = pd.concat(dfs, ignore_index=True)

        # Parse dates - handle various column names
        date_col = None
        for col in ['publication_date', 'pub_date', 'releasedate', 'release_date']:
            if col in combined.columns:
                date_col = col
                break

        if date_col is None:
            # Try to extract date from file names instead
            monthly_counts = self._aggregate_from_filenames(parquet_files)
        else:
            combined['pub_date'] = pd.to_datetime(combined[date_col], errors='coerce')
            combined = combined.dropna(subset=['pub_date'])
            combined['month'] = combined['pub_date'].dt.to_period('M')

            monthly = combined.groupby('month').size().reset_index(name='count')
            monthly['cumulative'] = monthly['count'].cumsum()
            monthly['date'] = monthly['month'].dt.to_timestamp().dt.strftime('%Y-%m-%d')
            monthly_counts = monthly

        # Build timeseries
        timeseries_data = [
            TimeseriesPoint(
                date=row['date'],
                value=int(row['count']),
                cumulative=int(row['cumulative'])
            )
            for _, row in monthly_counts.iterrows()
        ]

        current_total = int(monthly_counts['cumulative'].iloc[-1]) if len(monthly_counts) > 0 else 0

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="sequences",
                    name="Sequencing Runs",
                    unit="runs",
                    current_value=current_total,
                    description="Total genomic sequencing runs in SRA"
                )
            ],
            timeseries=[
                Timeseries(metric_id="sequences", data=timeseries_data)
            ],
            update_frequency="weekly",
            data_license="Public Domain"
        )

    def _aggregate_from_filenames(self, parquet_files: list) -> pd.DataFrame:
        """Fallback: aggregate counts from file date ranges."""
        records = []
        for f in sorted(parquet_files):
            basename = os.path.basename(f)
            try:
                # Parse: sra_search_YYYYMMDD_to_YYYYMMDD.parquet
                parts = basename.replace('.parquet', '').split('_')
                start_str = parts[2]
                df = pd.read_parquet(f)
                count = len(df)
                date = datetime.strptime(start_str, "%Y%m%d")
                records.append({'date': date.strftime('%Y-%m-%d'), 'count': count})
            except:
                continue

        df = pd.DataFrame(records)
        df['cumulative'] = df['count'].cumsum()
        return df
