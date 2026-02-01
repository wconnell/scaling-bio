"""Collector for NCBI GenBank sequence data."""

import os
import re
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class GenBankCollector(BaseCollector):
    """Collector for NCBI GenBank total bases.

    Scrapes historical growth data from NCBI GenBank Statistics page.
    """

    STATS_URL = "https://www.ncbi.nlm.nih.gov/genbank/statistics/"

    def __init__(self, data_dir: str = "data/genbank"):
        self.data_dir = data_dir

    @property
    def source_id(self) -> str:
        return "genbank"

    @property
    def source_info(self) -> SourceInfo:
        return SourceInfo(
            id="genbank",
            name="GenBank",
            description="NCBI's annotated collection of nucleotide sequences",
            url="https://www.ncbi.nlm.nih.gov/genbank/statistics/",
            color="#8b5cf6",
            icon="dna"
        )

    def collect(self) -> None:
        """Fetch GenBank statistics from NCBI."""
        os.makedirs(self.data_dir, exist_ok=True)

        print("  Fetching GenBank statistics...")

        response = requests.get(self.STATS_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the growth table - it contains Release, Date, Base Pairs, Sequences
        tables = soup.find_all('table')

        growth_data = []

        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]

            # Look for table with base pairs column
            if any('base' in h for h in headers):
                rows = table.find_all('tr')[1:]  # Skip header

                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        try:
                            # Parse release number
                            release_text = cols[0].get_text(strip=True)

                            # Parse date (format varies: "Dec 2024", "12/2024", etc.)
                            date_text = cols[1].get_text(strip=True)

                            # Parse base pairs (may have commas)
                            bp_text = cols[2].get_text(strip=True).replace(',', '')

                            # Try to extract year from date
                            year_match = re.search(r'(19|20)\d{2}', date_text)
                            if year_match:
                                year = int(year_match.group())
                                bases = int(bp_text)

                                # Parse sequences if available
                                sequences = 0
                                if len(cols) >= 4:
                                    seq_text = cols[3].get_text(strip=True).replace(',', '')
                                    try:
                                        sequences = int(seq_text)
                                    except ValueError:
                                        pass

                                growth_data.append({
                                    'release': release_text,
                                    'date': date_text,
                                    'year': year,
                                    'bases': bases,
                                    'sequences': sequences
                                })
                        except (ValueError, IndexError):
                            continue

                if growth_data:
                    break

        if not growth_data:
            raise ValueError("Could not parse GenBank statistics table")

        # Sort by year and deduplicate (keep latest per year)
        df = pd.DataFrame(growth_data)
        df = df.sort_values(['year', 'bases'])
        df = df.drop_duplicates(subset=['year'], keep='last')
        df = df.sort_values('year')

        df.to_parquet(os.path.join(self.data_dir, "genbank_growth.parquet"))

        latest = df.iloc[-1]
        print(f"  Latest: {latest['bases'] / 1e12:.1f} TB ({latest['sequences']:,} sequences)")

    def transform(self) -> CollectorOutput:
        """Transform GenBank data to standard format."""
        df = pd.read_parquet(os.path.join(self.data_dir, "genbank_growth.parquet"))

        # GenBank stats are already cumulative totals
        timeseries_data = []
        prev_bases = 0

        for _, row in df.iterrows():
            annual_bases = row['bases'] - prev_bases
            if annual_bases < 0:
                annual_bases = 0  # Handle any data anomalies

            timeseries_data.append(
                TimeseriesPoint(
                    date=f"{int(row['year'])}-01-01",
                    value=int(annual_bases),
                    cumulative=int(row['bases'])
                )
            )
            prev_bases = row['bases']

        current_total = int(df['bases'].iloc[-1])

        # Format as terabases
        terabases = current_total / 1e12
        if terabases >= 1:
            formatted = f"{terabases:.1f} TB"
        else:
            gigabases = current_total / 1e9
            formatted = f"{gigabases:.1f} GB"

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="bases",
                    name="Total Bases",
                    unit="bases",
                    current_value=current_total,
                    formatted_value=formatted,
                    description="Total annotated nucleotide bases in GenBank"
                )
            ],
            timeseries=[
                Timeseries(metric_id="bases", data=timeseries_data)
            ],
            update_frequency="bimonthly",
            data_license="Public Domain"
        )
