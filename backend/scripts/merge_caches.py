#!/usr/bin/env python3
"""
Merge two lineage cache JSON files.
Handles different cache formats:
- Base cache: objects as dict, dependencies as {"table_level": [...]}
- GitHub cache: objects as list, dependencies as list

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
        result = {}
        for obj in objects:
            # Get the key - could be object_id (string) or id
            key = obj.get("object_id") or obj.get("id")
            if key:
                result[key] = obj
        return result
    return objects if objects else {}


def get_deps_list(deps):
    """Extract dependencies as a flat list, handling nested structures."""
    if isinstance(deps, list):
        return deps
    if isinstance(deps, dict):
        # Check if it's {"table_level": [...], "column_level": [...]}
        if "table_level" in deps:
            return deps.get("table_level", [])
        # Otherwise treat as {id: dep} dict
        return list(deps.values())
    return []


def get_dep_key(dep):
    """Extract (source, target) tuple from a dependency."""
    source = dep.get("source_object_id") or dep.get("source_id") or dep.get("source")
    target = dep.get("target_object_id") or dep.get("target_id") or dep.get("target")
    return (source, target) if source and target else None


def merge_caches(base: dict, new: dict) -> tuple:
    """Merge two lineage caches. Handles different formats."""
    # Normalize objects to dict format
    base_objects = normalize_objects(base.get("objects", {}))
    new_objects = normalize_objects(new.get("objects", {}))

    # Merge objects
    added_objects = 0
    for obj_id, obj in new_objects.items():
        if obj_id not in base_objects:
            base_objects[obj_id] = obj
            added_objects += 1

    # Get dependencies as lists
    base_deps_list = get_deps_list(base.get("dependencies", []))
    new_deps_list = get_deps_list(new.get("dependencies", []))

    # Build set of existing deps
    existing_deps = set()
    for dep in base_deps_list:
        key = get_dep_key(dep)
        if key:
            existing_deps.add(key)

    # Merge dependencies
    added_deps = 0
    for dep in new_deps_list:
        key = get_dep_key(dep)
        if key and key not in existing_deps:
            base_deps_list.append(dep)
            existing_deps.add(key)
            added_deps += 1

    # Update base with merged data
    base["objects"] = base_objects

    # Preserve the original structure for dependencies
    if isinstance(base.get("dependencies"), dict) and "table_level" in base.get("dependencies", {}):
        base["dependencies"]["table_level"] = base_deps_list
    else:
        base["dependencies"] = base_deps_list

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
    before_deps = len(get_deps_list(base_cache.get("dependencies", [])))

    merged, added_objects, added_deps = merge_caches(base_cache, new_cache)

    after_objects = len(merged["objects"])
    after_deps = len(get_deps_list(merged.get("dependencies", [])))

    with open(args.output, "w") as f:
        json.dump(merged, f, indent=2)

    print(f"Merged: {args.new} -> {args.base}")
    print(f"Objects: {before_objects} -> {after_objects} (+{added_objects})")
    print(f"Dependencies: {before_deps} -> {after_deps} (+{added_deps})")
    print(f"Saved to: {args.output}")


if __name__ == "__main__":
    main()
