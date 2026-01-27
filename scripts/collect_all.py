#!/usr/bin/env python3
"""Run all collectors and generate site data."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.registry import get_all_collectors


def main():
    output_dir = Path("site/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    sources = []
    errors = []

    collectors = get_all_collectors()
    print(f"Running {len(collectors)} collectors...\n")

    for collector in collectors:
        print(f"[{collector.source_id}] Starting...")
        try:
            output_path = collector.run(str(output_dir))
            sources.append(collector.source_id)
            print(f"[{collector.source_id}] Success: {output_path}\n")
        except Exception as e:
            print(f"[{collector.source_id}] Error: {e}\n")
            errors.append((collector.source_id, str(e)))

    # Generate manifest
    manifest = {
        "version": "1.0.0",
        "generated": datetime.utcnow().isoformat() + "Z",
        "sources": sources
    }

    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print("=" * 50)
    print(f"Generated manifest: {manifest_path}")
    print(f"Sources: {len(sources)}, Errors: {len(errors)}")

    if errors:
        print("\nErrors:")
        for source_id, error in errors:
            print(f"  {source_id}: {error}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
