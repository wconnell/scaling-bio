"""Collector for NCBI GenBank sequence data."""

import os
import re
import time
from datetime import datetime
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .base import (
    BaseCollector, CollectorOutput, SourceInfo,
    Metric, Timeseries, TimeseriesPoint
)


class GenBankCollector(BaseCollector):
    """Collector for NCBI GenBank total bases.

    Fetches statistics from FTP release notes files.
    """

    FTP_BASE = "https://ftp.ncbi.nih.gov/genbank/release.notes"

    def __init__(self, data_dir: str = "data/genbank"):
        self.data_dir = data_dir

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((requests.exceptions.RequestException,))
    )
    def _fetch_url(self, url: str) -> requests.Response:
        """Fetch URL with retry logic."""
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response

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
        """Fetch GenBank statistics from FTP release notes."""
        os.makedirs(self.data_dir, exist_ok=True)

        print("  Fetching GenBank release notes...")

        # Get list of release notes files
        response = self._fetch_url(f"{self.FTP_BASE}/")

        # Parse release numbers from directory listing
        release_pattern = re.compile(r'gb(\d+)\.release\.notes')
        releases = sorted(set(int(m.group(1)) for m in release_pattern.finditer(response.text)))

        print(f"    Found {len(releases)} releases (gb{releases[0]} to gb{releases[-1]})")

        growth_data = []

        # Sample releases: every ~10 releases for history, plus recent ones
        sampled = []
        for r in releases:
            if r <= 150 and r % 10 == 0:  # Early: every 10
                sampled.append(r)
            elif r <= 230 and r % 5 == 0:  # Mid: every 5
                sampled.append(r)
            elif r > 230:  # Recent: all
                sampled.append(r)

        # Always include first and last
        if releases[0] not in sampled:
            sampled.insert(0, releases[0])
        if releases[-1] not in sampled:
            sampled.append(releases[-1])

        sampled = sorted(set(sampled))
        print(f"    Sampling {len(sampled)} releases...")

        for release_num in sampled:
            url = f"{self.FTP_BASE}/gb{release_num}.release.notes"
            try:
                time.sleep(0.5)  # Be polite to NCBI servers
                resp = self._fetch_url(url)
                text = resp.text[:5000]  # Only need header

                # Extract date from header (e.g., "June 15, 2025")
                date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(\d{4})', text)
                year = int(date_match.group(2)) if date_match else None

                # Extract bases - look for "X bases" in traditional records section
                # Format: "258,320,620 sequences\n5,676,067,778,413 bases"
                bases_match = re.search(r'([\d,]+)\s+bases', text)
                bases = int(bases_match.group(1).replace(',', '')) if bases_match else None

                # Extract sequences
                seq_match = re.search(r'([\d,]+)\s+sequences', text)
                sequences = int(seq_match.group(1).replace(',', '')) if seq_match else None

                if year and bases:
                    growth_data.append({
                        'release': release_num,
                        'year': year,
                        'bases': bases,
                        'sequences': sequences or 0
                    })
                    print(f"      gb{release_num} ({year}): {bases/1e12:.2f} TB")

            except Exception as e:
                print(f"      gb{release_num}: failed ({e})")
                continue

        if not growth_data:
            raise ValueError("Could not fetch any GenBank release notes")

        # Sort and deduplicate by year (keep latest release per year)
        df = pd.DataFrame(growth_data)
        df = df.sort_values(['year', 'release'])
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
