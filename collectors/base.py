"""Base collector class and data structures."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Any
import json
import os


@dataclass
class Metric:
    """A single metric tracked by a data source."""
    id: str
    name: str
    unit: str
    current_value: float
    formatted_value: Optional[str] = None
    description: Optional[str] = None


@dataclass
class TimeseriesPoint:
    """A single point in a timeseries."""
    date: str  # ISO format YYYY-MM-DD
    cumulative: float
    value: Optional[float] = None


@dataclass
class Timeseries:
    """A timeseries of data points for a metric."""
    metric_id: str
    data: List[TimeseriesPoint]


@dataclass
class SourceInfo:
    """Metadata about a data source."""
    id: str
    name: str
    description: str
    url: str
    color: str = "#3b82f6"
    icon: Optional[str] = None


@dataclass
class CollectorOutput:
    """Standard output format for all collectors."""
    source: SourceInfo
    metrics: List[Metric]
    timeseries: List[Timeseries]
    last_updated: datetime = field(default_factory=datetime.utcnow)
    update_frequency: str = "weekly"
    data_license: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary matching the JSON schema."""
        return {
            "source": {
                "id": self.source.id,
                "name": self.source.name,
                "description": self.source.description,
                "url": self.source.url,
                "color": self.source.color,
                "icon": self.source.icon
            },
            "metrics": [
                {
                    "id": m.id,
                    "name": m.name,
                    "unit": m.unit,
                    "current_value": m.current_value,
                    "formatted_value": m.formatted_value or self._format_number(m.current_value),
                    "description": m.description
                }
                for m in self.metrics
            ],
            "timeseries": [
                {
                    "metric_id": ts.metric_id,
                    "data": [
                        {"date": p.date, "cumulative": p.cumulative, "value": p.value}
                        for p in ts.data
                    ]
                }
                for ts in self.timeseries
            ],
            "metadata": {
                "last_updated": self.last_updated.isoformat() + "Z",
                "update_frequency": self.update_frequency,
                "data_license": self.data_license
            }
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @staticmethod
    def _format_number(n: float) -> str:
        """Format a number with appropriate suffix (K, M, B, T)."""
        if n >= 1e12:
            return f"{n/1e12:.1f}T"
        elif n >= 1e9:
            return f"{n/1e9:.1f}B"
        elif n >= 1e6:
            return f"{n/1e6:.1f}M"
        elif n >= 1e3:
            return f"{n/1e3:.1f}K"
        return str(int(n))


class BaseCollector(ABC):
    """Abstract base class for all data collectors."""

    @property
    @abstractmethod
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        pass

    @property
    @abstractmethod
    def source_info(self) -> SourceInfo:
        """Metadata about this data source."""
        pass

    @abstractmethod
    def collect(self) -> None:
        """Fetch raw data and store in data/ directory."""
        pass

    @abstractmethod
    def transform(self) -> CollectorOutput:
        """Transform raw data to standard output format."""
        pass

    def run(self, output_dir: str = "site/data") -> str:
        """Execute collection and transformation, return output path."""
        self.collect()
        output = self.transform()

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{self.source_id}.json")

        with open(output_path, 'w') as f:
            f.write(output.to_json())

        return output_path
