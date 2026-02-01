"""Registry for data collectors."""

from typing import Dict, Type, List
from .base import BaseCollector


# Registry of all available collectors - populated by imports
COLLECTORS: Dict[str, Type[BaseCollector]] = {}


def register_collector(source_id: str, collector_class: Type[BaseCollector]):
    """Register a new collector."""
    COLLECTORS[source_id] = collector_class


def get_collector(source_id: str) -> BaseCollector:
    """Get a collector instance by source ID."""
    if source_id not in COLLECTORS:
        raise ValueError(f"Unknown collector: {source_id}. Available: {list(COLLECTORS.keys())}")
    return COLLECTORS[source_id]()


def get_all_collectors() -> List[BaseCollector]:
    """Get instances of all registered collectors."""
    return [cls() for cls in COLLECTORS.values()]


# Import collectors to register them
def _register_all():
    """Import all collector modules to register them."""
    try:
        from .sra_collector import SRACollector
        register_collector("sra", SRACollector)
    except ImportError as e:
        print(f"Warning: Could not import SRA collector: {e}")

    try:
        from .cellxgene_collector import CellxGeneCollector
        register_collector("cellxgene", CellxGeneCollector)
    except ImportError as e:
        print(f"Warning: Could not import CellxGene collector: {e}")

    try:
        from .pdb_collector import PDBCollector
        register_collector("pdb", PDBCollector)
    except ImportError as e:
        print(f"Warning: Could not import PDB collector: {e}")

    try:
        from .genbank_collector import GenBankCollector
        register_collector("genbank", GenBankCollector)
    except ImportError as e:
        print(f"Warning: Could not import GenBank collector: {e}")

    try:
        from .uniprot_collector import UniProtCollector
        register_collector("uniprot", UniProtCollector)
    except ImportError as e:
        print(f"Warning: Could not import UniProt collector: {e}")


_register_all()
