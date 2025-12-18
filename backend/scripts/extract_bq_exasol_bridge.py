#!/usr/bin/env python3
"""
Extract BQ-to-Exasol sync metadata to bridge lineage between platforms.

This script queries the BQ sync metadata table and creates lineage dependencies:
  BigQuery table -> Exasol STG table -> Exasol DM table

Usage:
    python extract_bq_exasol_bridge.py --project PROJECT --dataset DATASET --table TABLE --output bridge_lineage.json
    python extract_bq_exasol_bridge.py --project PROJECT --dataset DATASET --table TABLE --merge-with lineage_cache.json --output lineage_cache.json

Environment:
    GOOGLE_APPLICATION_CREDENTIALS: Path to service account JSON (or use gcloud auth)
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_sync_metadata(project: str, dataset: str, table: str) -> list[dict]:
    """Fetch sync metadata from BigQuery."""
    client = bigquery.Client(project=project)

    query = f"""
    SELECT
        batch_name,
        task_name,
        table_type,
        bq_project_id,
        bq_dataset_id,
        bq_table_name,
        exa_stg_schema_name,
        exa_stg_table_name,
        exa_dm_schema_name,
        exa_dm_table_name,
        is_snapshot
    FROM `{project}.{dataset}.{table}`
    """

    logger.info(f"Querying {project}.{dataset}.{table}...")

    results = []
    query_job = client.query(query)

    for row in query_job:
        results.append({
            "batch_name": row.batch_name,
            "task_name": row.task_name,
            "table_type": row.table_type,
            "bq_project_id": row.bq_project_id,
            "bq_dataset_id": row.bq_dataset_id,
            "bq_table_name": row.bq_table_name,
            "exa_stg_schema_name": row.exa_stg_schema_name,
            "exa_stg_table_name": row.exa_stg_table_name,
            "exa_dm_schema_name": row.exa_dm_schema_name,
            "exa_dm_table_name": row.exa_dm_table_name,
            "is_snapshot": row.is_snapshot,
        })

    logger.info(f"Found {len(results)} sync mappings")
    return results


def build_lineage_from_sync(sync_records: list[dict]) -> dict:
    """Build lineage objects and dependencies from sync metadata."""
    objects = {}
    dependencies = []

    for record in sync_records:
        # Skip if missing required fields
        if not record.get("bq_table_name"):
            continue

        # Build BigQuery object ID
        bq_project = record.get("bq_project_id", "")
        bq_dataset = record.get("bq_dataset_id", "")
        bq_table = record["bq_table_name"]

        if bq_project and bq_dataset:
            bq_full_name = f"{bq_project}.{bq_dataset}.{bq_table}"
        elif bq_dataset:
            bq_full_name = f"{bq_dataset}.{bq_table}"
        else:
            bq_full_name = bq_table

        bq_object_id = f"BIGQUERY.{bq_full_name}".upper()

        # Create BQ object
        if bq_object_id not in objects:
            objects[bq_object_id] = {
                "id": bq_object_id,
                "object_id": bq_object_id,
                "name": bq_table,
                "schema": bq_dataset,
                "type": "TABLE",
                "platform": "bigquery",
                "project": bq_project,
            }

        # Build Exasol STG object (if exists)
        exa_stg_schema = record.get("exa_stg_schema_name", "")
        exa_stg_table = record.get("exa_stg_table_name", "")
        exa_stg_object_id = None

        if exa_stg_schema and exa_stg_table:
            exa_stg_object_id = f"{exa_stg_schema}.{exa_stg_table}".upper()

            if exa_stg_object_id not in objects:
                objects[exa_stg_object_id] = {
                    "id": exa_stg_object_id,
                    "object_id": exa_stg_object_id,
                    "name": exa_stg_table,
                    "schema": exa_stg_schema,
                    "type": "TABLE",
                    "platform": "exasol",
                    "layer": "STG",
                }

            # Dependency: BQ -> Exasol STG
            dependencies.append({
                "source_id": bq_object_id,
                "target_id": exa_stg_object_id,
                "dependency_type": "SYNC",
                "reference_type": "BQ_TO_EXASOL",
                "batch_name": record.get("batch_name", ""),
                "task_name": record.get("task_name", ""),
            })

        # Build Exasol DM object (if exists)
        exa_dm_schema = record.get("exa_dm_schema_name", "")
        exa_dm_table = record.get("exa_dm_table_name", "")

        if exa_dm_schema and exa_dm_table:
            exa_dm_object_id = f"{exa_dm_schema}.{exa_dm_table}".upper()

            if exa_dm_object_id not in objects:
                objects[exa_dm_object_id] = {
                    "id": exa_dm_object_id,
                    "object_id": exa_dm_object_id,
                    "name": exa_dm_table,
                    "schema": exa_dm_schema,
                    "type": "TABLE",
                    "platform": "exasol",
                    "layer": "DM",
                }

            # Dependency: STG -> DM (if STG exists) or BQ -> DM (if no STG)
            if exa_stg_object_id:
                dependencies.append({
                    "source_id": exa_stg_object_id,
                    "target_id": exa_dm_object_id,
                    "dependency_type": "ETL",
                    "reference_type": "STG_TO_DM",
                })
            else:
                dependencies.append({
                    "source_id": bq_object_id,
                    "target_id": exa_dm_object_id,
                    "dependency_type": "SYNC",
                    "reference_type": "BQ_TO_EXASOL",
                    "batch_name": record.get("batch_name", ""),
                    "task_name": record.get("task_name", ""),
                })

    return {
        "metadata": {
            "source": "bq_exasol_bridge",
            "extracted_at": datetime.now().isoformat(),
            "record_count": len(sync_records),
        },
        "objects": list(objects.values()),
        "dependencies": dependencies,
    }


def merge_into_cache(base_path: str, new_data: dict) -> dict:
    """Merge bridge data into existing cache."""
    with open(base_path) as f:
        base = json.load(f)

    # Normalize base objects to dict if needed
    base_objects = base.get("objects", {})
    if isinstance(base_objects, list):
        base_objects = {obj.get("id") or obj.get("object_id"): obj for obj in base_objects}

    # Add new objects
    added_objects = 0
    for obj in new_data["objects"]:
        obj_id = obj.get("id") or obj.get("object_id")
        if obj_id and obj_id not in base_objects:
            base_objects[obj_id] = obj
            added_objects += 1

    base["objects"] = base_objects

    # Get base dependencies list
    base_deps = base.get("dependencies", [])
    if isinstance(base_deps, dict) and "table_level" in base_deps:
        base_deps_list = base_deps["table_level"]
    elif isinstance(base_deps, list):
        base_deps_list = base_deps
    else:
        base_deps_list = []

    # Build existing deps set
    existing_deps = set()
    for dep in base_deps_list:
        source = dep.get("source_id") or dep.get("source_object_id")
        target = dep.get("target_id") or dep.get("target_object_id")
        if source and target:
            existing_deps.add((source, target))

    # Add new dependencies
    added_deps = 0
    for dep in new_data["dependencies"]:
        source = dep.get("source_id") or dep.get("source_object_id")
        target = dep.get("target_id") or dep.get("target_object_id")
        if source and target and (source, target) not in existing_deps:
            base_deps_list.append(dep)
            existing_deps.add((source, target))
            added_deps += 1

    # Update dependencies in original structure
    if isinstance(base.get("dependencies"), dict) and "table_level" in base.get("dependencies", {}):
        base["dependencies"]["table_level"] = base_deps_list
    else:
        base["dependencies"] = base_deps_list

    # Update metadata
    base["metadata"]["bridge_merged_at"] = datetime.now().isoformat()
    base["metadata"]["bridge_stats"] = {
        "objects_added": added_objects,
        "dependencies_added": added_deps,
    }

    logger.info(f"Merged: +{added_objects} objects, +{added_deps} dependencies")

    return base


def main():
    parser = argparse.ArgumentParser(description="Extract BQ-to-Exasol sync metadata for lineage")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset")
    parser.add_argument("--table", required=True, help="Sync metadata table name")
    parser.add_argument("--output", default="bridge_lineage.json", help="Output file")
    parser.add_argument("--merge-with", help="Existing cache to merge into")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Fetch sync metadata from BQ
    sync_records = fetch_sync_metadata(args.project, args.dataset, args.table)

    if not sync_records:
        logger.warning("No sync records found")
        return

    # Build lineage
    lineage_data = build_lineage_from_sync(sync_records)

    logger.info(f"Built {len(lineage_data['objects'])} objects, {len(lineage_data['dependencies'])} dependencies")

    # Merge or save directly
    if args.merge_with and Path(args.merge_with).exists():
        result = merge_into_cache(args.merge_with, lineage_data)
    else:
        result = lineage_data

    # Save
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    logger.info(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
