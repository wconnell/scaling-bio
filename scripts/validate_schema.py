#!/usr/bin/env python3
"""Validate all JSON files against the schema."""

import json
import sys
from pathlib import Path

try:
    from jsonschema import validate, ValidationError
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False
    print("Warning: jsonschema not installed, using basic validation")


SCHEMA = {
    "$schema": "https://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["source", "metrics", "timeseries", "metadata"],
    "properties": {
        "source": {
            "type": "object",
            "required": ["id", "name", "description", "url"],
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "description": {"type": "string"},
                "url": {"type": "string"},
                "color": {"type": "string"},
                "icon": {"type": "string"}
            }
        },
        "metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "name", "unit", "current_value"],
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "unit": {"type": "string"},
                    "current_value": {"type": "number"},
                    "formatted_value": {"type": "string"},
                    "description": {"type": "string"}
                }
            }
        },
        "timeseries": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["metric_id", "data"],
                "properties": {
                    "metric_id": {"type": "string"},
                    "data": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["date", "cumulative"],
                            "properties": {
                                "date": {"type": "string"},
                                "value": {"type": ["number", "null"]},
                                "cumulative": {"type": "number"}
                            }
                        }
                    }
                }
            }
        },
        "metadata": {
            "type": "object",
            "required": ["last_updated"],
            "properties": {
                "last_updated": {"type": "string"},
                "update_frequency": {"type": "string"},
                "data_license": {"type": "string"}
            }
        }
    }
}


def validate_basic(data: dict) -> list:
    """Basic validation without jsonschema."""
    errors = []

    required_keys = ["source", "metrics", "timeseries", "metadata"]
    for key in required_keys:
        if key not in data:
            errors.append(f"Missing required key: {key}")

    if "source" in data:
        for key in ["id", "name", "description", "url"]:
            if key not in data["source"]:
                errors.append(f"Missing source.{key}")

    if "metrics" in data:
        if not isinstance(data["metrics"], list):
            errors.append("metrics must be an array")
        else:
            for i, metric in enumerate(data["metrics"]):
                for key in ["id", "name", "unit", "current_value"]:
                    if key not in metric:
                        errors.append(f"Missing metrics[{i}].{key}")

    if "timeseries" in data:
        if not isinstance(data["timeseries"], list):
            errors.append("timeseries must be an array")
        else:
            for i, ts in enumerate(data["timeseries"]):
                if "metric_id" not in ts:
                    errors.append(f"Missing timeseries[{i}].metric_id")
                if "data" not in ts:
                    errors.append(f"Missing timeseries[{i}].data")

    if "metadata" in data:
        if "last_updated" not in data["metadata"]:
            errors.append("Missing metadata.last_updated")

    return errors


def main():
    data_dir = Path("site/data")
    errors = []

    json_files = list(data_dir.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return 1

    for json_file in json_files:
        if json_file.name == "manifest.json":
            continue

        print(f"Validating {json_file.name}...")

        try:
            with open(json_file) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  -> Invalid JSON: {e}")
            errors.append((json_file.name, f"Invalid JSON: {e}"))
            continue

        if HAS_JSONSCHEMA:
            try:
                validate(instance=data, schema=SCHEMA)
                print(f"  -> Valid")
            except ValidationError as e:
                print(f"  -> Invalid: {e.message}")
                errors.append((json_file.name, e.message))
        else:
            validation_errors = validate_basic(data)
            if validation_errors:
                for err in validation_errors:
                    print(f"  -> {err}")
                errors.append((json_file.name, "; ".join(validation_errors)))
            else:
                print(f"  -> Valid (basic check)")

    if errors:
        print(f"\n{len(errors)} validation errors")
        return 1

    print("\nAll files valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
