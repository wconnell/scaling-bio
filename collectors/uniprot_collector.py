"""Collector for UniProt protein sequence data."""

import os
import re
from datetime import datetime
import pandas as pd
import requests

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class UniProtCollector(BaseCollector):
    """Collector for UniProt protein sequence counts.

    Uses UniProt REST API and release statistics.
    """

    STATS_URL = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/reldate.txt"
    RELEASE_NOTES_URL = "https://ftp.uniprot.org/pub/databases/uniprot/relnotes.txt"
    API_URL = "https://rest.uniprot.org/uniprotkb/search"

    def __init__(self, data_dir: str = "data/uniprot"):
        self.data_dir = data_dir

    @property
    def source_id(self) -> str:
        return "uniprot"

    @property
    def source_info(self) -> SourceInfo:
        return SourceInfo(
            id="uniprot",
            name="UniProt",
            description="Universal Protein Resource - comprehensive protein sequences",
            url="https://www.uniprot.org/uniprotkb/statistics",
            color="#f59e0b",
            icon="chain"
        )

    def collect(self) -> None:
        """Fetch UniProt statistics from release notes."""
        os.makedirs(self.data_dir, exist_ok=True)

        print("  Fetching UniProt release statistics...")

        # Fetch release notes which contain historical counts
        response = requests.get(self.RELEASE_NOTES_URL, timeout=30)
        response.raise_for_status()

        content = response.text

        # Parse release notes for historical data
        # Format: "UniProt Knowledgebase Release 2024_01 statistics"
        # followed by counts

        yearly_data = {}

        # Pattern for release headers and statistics
        # Looking for lines like:
        # "UniProt Release 2024_06" or "UniProtKB/Swiss-Prot Release 2024_06"
        release_pattern = r'(?:UniProt(?:KB)?|Swiss-Prot|TrEMBL).*?Release\s+(\d{4})_(\d+)'

        # Pattern for sequence counts
        # Various formats: "571,609 sequence entries" or "Swiss-Prot: 571,609"
        count_pattern = r'([\d,]+)\s+(?:sequence\s+)?entries'

        lines = content.split('\n')
        current_year = None
        current_entries = 0

        for i, line in enumerate(lines):
            # Check for release header
            release_match = re.search(release_pattern, line, re.IGNORECASE)
            if release_match:
                year = int(release_match.group(1))
                current_year = year

            # Check for entry count (total UniProtKB entries)
            if current_year and 'UniProtKB' in line and 'entries' in line.lower():
                count_match = re.search(r'([\d,]+)\s*entries', line, re.IGNORECASE)
                if count_match:
                    entries = int(count_match.group(1).replace(',', ''))
                    if current_year not in yearly_data or entries > yearly_data[current_year]:
                        yearly_data[current_year] = entries

        # If parsing release notes didn't work well, use API to get current count
        # and historical data from known milestones
        if len(yearly_data) < 5:
            print("  Fetching from UniProt API and known milestones...")
            yearly_data = self._get_historical_data()

        # Convert to DataFrame
        growth_data = []
        for year in sorted(yearly_data.keys()):
            growth_data.append({
                'year': year,
                'sequences': yearly_data[year]
            })

        df = pd.DataFrame(growth_data)
        df.to_parquet(os.path.join(self.data_dir, "uniprot_growth.parquet"))

        latest = df.iloc[-1]
        print(f"  Latest: {latest['sequences'] / 1e6:.1f}M sequences")

    def _get_historical_data(self) -> dict:
        """Fetch historical UniProt data from FTP release archives.

        Primary source: https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/
        Each release has a relnotes.txt with total entry counts.
        Uses year-end releases (month 12) when available for accurate yearly totals.
        """
        historical = {}
        current_year = datetime.now().year
        base_url = "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases"

        for year in range(2011, current_year + 1):
            # Try year-end release first (_12), then fall back to _01
            for month in ['12', '01']:
                release_id = f"release-{year}_{month}"
                url = f"{base_url}/{release_id}/relnotes.txt"

                try:
                    response = requests.get(url, timeout=15)
                    if response.status_code == 200:
                        # Parse entry count: "consists of N entries" or "N entries"
                        match = re.search(r'(\d[\d,]*)\s+entries\s*\(UniProtKB', response.text)
                        if not match:
                            match = re.search(r'consists?\s+of\s+([\d,]+)\s+entries', response.text, re.IGNORECASE)
                        if match:
                            count = int(match.group(1).replace(',', ''))
                            historical[year] = count
                            print(f"    {year}: {count:,} entries")
                            break  # Got data for this year
                except Exception:
                    pass

        # Get current count from API
        try:
            response = requests.get(
                self.API_URL,
                params={'query': '*', 'size': '0'},
                headers={'Accept': 'application/json'},
                timeout=30
            )
            if response.status_code == 200:
                total = response.headers.get('X-Total-Results')
                if total:
                    total_int = int(total)
                    # Only use API value if it's higher than previous year (sanity check)
                    max_historical = max(historical.values()) if historical else 0
                    if total_int >= max_historical:
                        historical[current_year] = total_int
                        print(f"    {current_year}: {total_int:,} entries (current)")
                    else:
                        print(f"    {current_year}: API returned {total_int:,} (lower than previous, skipping)")
        except Exception as e:
            print(f"  Warning: Could not fetch current count: {e}")

        return historical

    def transform(self) -> CollectorOutput:
        """Transform UniProt data to standard format."""
        df = pd.read_parquet(os.path.join(self.data_dir, "uniprot_growth.parquet"))

        # Calculate annual additions
        timeseries_data = []
        prev_seqs = 0

        for _, row in df.iterrows():
            annual_seqs = row['sequences'] - prev_seqs
            if annual_seqs < 0:
                annual_seqs = 0

            timeseries_data.append(
                TimeseriesPoint(
                    date=f"{int(row['year'])}-01-01",
                    value=int(annual_seqs),
                    cumulative=int(row['sequences'])
                )
            )
            prev_seqs = row['sequences']

        current_total = int(df['sequences'].iloc[-1])

        # Format as millions
        millions = current_total / 1e6
        formatted = f"{millions:.0f}M"

        return CollectorOutput(
            source=self.source_info,
            metrics=[
                Metric(
                    id="sequences",
                    name="Protein Sequences",
                    unit="sequences",
                    current_value=current_total,
                    formatted_value=formatted,
                    description="Total protein sequences in UniProtKB"
                )
            ],
            timeseries=[
                Timeseries(metric_id="sequences", data=timeseries_data)
            ],
            update_frequency="monthly",
            data_license="CC BY 4.0"
        )
