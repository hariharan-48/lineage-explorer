#!/usr/bin/env python3
"""
Merge two lineage cache JSON files.
Handles both dict-based and list-based object formats.

Usage:
    python merge_caches.py --base lineage_cache.json --new github_lineage.json --output merged.json
"""
import argparse
import json
from datetime import datetime
from pathlib import Path


def normalize_objects(objects):
    """Convert objects to dict format if it's a list."""
    if isinstance(objects, list):
        return {obj.get("object_id") or obj.get("id"): obj for obj in objects}
    return objects


def normalize_dependencies(deps):
    """Convert dependencies to list format if needed."""
    if isinstance(deps, dict):
        return list(deps.values())
    return deps


def merge_caches(base: dict, new: dict) -> dict:
    """Merge two lineage caches. Handles both dict and list formats."""
    # Normalize objects to dict format
    base_objects = normalize_objects(base.get("objects", {}))
    new_objects = normalize_objects(new.get("objects", {}))

    # Merge objects
    added_objects = 0
    for obj_id, obj in new_objects.items():
        if obj_id not in base_objects:
            base_objects[obj_id] = obj
            added_objects += 1

    # Normalize dependencies to list format
    base_deps = normalize_dependencies(base.get("dependencies", []))
    new_deps = normalize_dependencies(new.get("dependencies", []))

    # Build set of existing deps
    existing_deps = set()
    for d in base_deps:
        source = d.get("source_object_id") or d.get("source_id") or d.get("source")
        target = d.get("target_object_id") or d.get("target_id") or d.get("target")
        if source and target:
            existing_deps.add((source, target))

    # Merge dependencies
    added_deps = 0
    for dep in new_deps:
        source = dep.get("source_object_id") or dep.get("source_id") or dep.get("source")
        target = dep.get("target_object_id") or dep.get("target_id") or dep.get("target")
        if source and target:
            key = (source, target)
            if key not in existing_deps:
                base_deps.append(dep)
                existing_deps.add(key)
                added_deps += 1

    # Update base with merged data
    base["objects"] = base_objects
    base["dependencies"] = base_deps

    # Update metadata
    base["metadata"]["merged_at"] = datetime.now().isoformat()
    if "stats" in new.get("metadata", {}):
        base["metadata"]["github_stats"] = new["metadata"]["stats"]

    return base, added_objects, added_deps


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

    before_objects = len(normalize_objects(base_cache.get("objects", {})))
    before_deps = len(normalize_dependencies(base_cache.get("dependencies", [])))

    merged, added_objects, added_deps = merge_caches(base_cache, new_cache)

    after_objects = len(merged["objects"])
    after_deps = len(merged["dependencies"])

    with open(args.output, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"Merged: {args.new} -> {args.base}")
    print(f"Objects: {before_objects} -> {after_objects} (+{added_objects})")
    print(f"Dependencies: {before_deps} -> {after_deps} (+{added_deps})")
    print(f"Saved to: {args.output}")


if __name__ == "__main__":
    main()
