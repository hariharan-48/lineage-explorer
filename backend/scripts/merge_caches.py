#!/usr/bin/env python3
"""
Merge two lineage cache JSON files.

Usage:
    python merge_caches.py --base lineage_cache.json --new github_lineage.json --output merged.json
"""
import argparse
import json
from datetime import datetime
from pathlib import Path


def merge_caches(base: dict, new: dict) -> dict:
    """Merge two lineage caches."""
    # Merge objects (avoid duplicates by object_id)
    existing_ids = {obj["object_id"] for obj in base.get("objects", [])}
    for obj in new.get("objects", []):
        if obj["object_id"] not in existing_ids:
            base["objects"].append(obj)
            existing_ids.add(obj["object_id"])

    # Merge dependencies (avoid duplicates)
    existing_deps = {
        (d["source_object_id"], d["target_object_id"])
        for d in base.get("dependencies", [])
    }
    for dep in new.get("dependencies", []):
        key = (dep["source_object_id"], dep["target_object_id"])
        if key not in existing_deps:
            base["dependencies"].append(dep)
            existing_deps.add(key)

    # Update metadata
    base["metadata"]["merged_at"] = datetime.now().isoformat()
    if "stats" in new.get("metadata", {}):
        base["metadata"]["github_stats"] = new["metadata"]["stats"]

    return base


def main():
    parser = argparse.ArgumentParser(description="Merge two lineage cache files")
    parser.add_argument("--base", required=True, help="Base cache file")
    parser.add_argument("--new", required=True, help="New cache file to merge in")
    parser.add_argument("--output", required=True, help="Output file")
    args = parser.parse_args()

    with open(args.base) as f:
        base_cache = json.load(f)
    
    with open(args.new) as f:
        new_cache = json.load(f)

    before_objects = len(base_cache.get("objects", []))
    before_deps = len(base_cache.get("dependencies", []))

    merged = merge_caches(base_cache, new_cache)

    after_objects = len(merged["objects"])
    after_deps = len(merged["dependencies"])

    with open(args.output, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"Merged: {args.new} -> {args.base}")
    print(f"Objects: {before_objects} -> {after_objects} (+{after_objects - before_objects})")
    print(f"Dependencies: {before_deps} -> {after_deps} (+{after_deps - before_deps})")
    print(f"Saved to: {args.output}")


if __name__ == "__main__":
    main()
