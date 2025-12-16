#!/usr/bin/env python3
"""
BigQuery Lineage Extractor
==========================
Extracts database metadata and dependencies from BigQuery
to build the lineage cache. Compatible with the same cache format
as Exasol extractor for unified lineage visualization.

Usage:
    python extract_from_bigquery.py [--config path/to/config.yaml]

Requirements:
    pip install google-cloud-bigquery pyyaml
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

try:
    import yaml
except ImportError:
    print("PyYAML not installed. Run: pip install pyyaml")
    sys.exit(1)

try:
    from google.cloud import bigquery
except ImportError:
    print("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
    sys.exit(1)

# Import AST-based parser (shared with Exasol extractor)
try:
    from script_parser import SQLParser
    HAS_AST_PARSER = True
except ImportError:
    HAS_AST_PARSER = False
    print("Warning: script_parser not found. View parsing will be limited.")


class BigQueryLineageExtractor:
    """
    Extracts lineage data from BigQuery using INFORMATION_SCHEMA:
    - INFORMATION_SCHEMA.TABLES
    - INFORMATION_SCHEMA.VIEWS
    - INFORMATION_SCHEMA.ROUTINES (for UDFs/stored procedures)

    Supports multiple projects and datasets.
    """

    def __init__(self, config: dict):
        self.config = config
        self.client: Optional[bigquery.Client] = None
        self.objects: Dict[str, dict] = {}
        self.table_deps: List[dict] = []
        self.object_counter = 200000  # Start at 200000 to avoid collision with Exasol IDs
        self.projects_extracted: List[str] = []

    def connect(self, project_id: Optional[str] = None) -> None:
        """Establish connection to BigQuery for a specific project."""
        conn_config = self.config.get("connection", {})

        # Use provided project_id or fall back to config
        target_project = project_id or conn_config.get("project_id")
        credentials_file = conn_config.get("credentials_file")

        # Support environment variable for credentials
        if credentials_file and os.path.exists(credentials_file):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file

        print(f"Connecting to BigQuery project: {target_project or 'default'}...")

        if target_project:
            self.client = bigquery.Client(project=target_project)
        else:
            self.client = bigquery.Client()

        print(f"Connected to project: {self.client.project}")

    def disconnect(self) -> None:
        """Close BigQuery client."""
        if self.client:
            self.client.close()
            print("Disconnected from BigQuery.")

    def _next_id(self) -> int:
        """Generate next unique object ID."""
        self.object_counter += 1
        return self.object_counter

    def _make_object_id(self, project: str, dataset: str, table: str) -> str:
        """Create a unique object ID with bigquery prefix."""
        # Format: bigquery:project.dataset.table
        return f"bigquery:{project}.{dataset}.{table}"

    def _should_include_dataset(self, dataset: str) -> bool:
        """Check if dataset should be included based on config."""
        extraction = self.config.get("extraction", {})
        include = extraction.get("include_datasets", [])
        exclude = extraction.get("exclude_datasets", [])

        if include and dataset not in include:
            return False
        if exclude and dataset in exclude:
            return False
        return True

    def extract(self) -> dict:
        """Main extraction process. Supports multiple projects."""
        extraction_config = self.config.get("extraction", {})

        # Support multiple projects
        projects = extraction_config.get("projects", [])
        if not projects:
            # Fall back to single project from connection config
            projects = [self.config.get("connection", {}).get("project_id")]

        for project_id in projects:
            if not project_id:
                continue

            try:
                self.connect(project_id)
                self.projects_extracted.append(project_id)

                # Get datasets for this project
                project_datasets = extraction_config.get("datasets", {})

                # datasets can be:
                # 1. A list (applies to all projects): ["dataset1", "dataset2"]
                # 2. A dict mapping project to datasets: {"project1": ["ds1"], "project2": ["ds2"]}
                if isinstance(project_datasets, dict):
                    datasets = project_datasets.get(project_id, [])
                elif isinstance(project_datasets, list):
                    datasets = project_datasets
                else:
                    datasets = []

                if not datasets:
                    # Get all datasets in project
                    datasets = [ds.dataset_id for ds in self.client.list_datasets()]
                    print(f"Found {len(datasets)} datasets in project {project_id}")

                # Filter datasets
                datasets = [d for d in datasets if self._should_include_dataset(d)]
                print(f"Processing {len(datasets)} datasets after filtering")

                # Extract from each dataset
                for dataset in datasets:
                    print(f"\nProcessing dataset: {project_id}.{dataset}")
                    self._extract_tables(dataset)
                    self._extract_views(dataset)

                    if extraction_config.get("include_routines", True):
                        self._extract_routines(dataset)

            except Exception as e:
                print(f"Error processing project {project_id}: {e}")
            finally:
                self.disconnect()

        # Parse view and routine definitions for dependencies
        if self.config.get("script_parsing", {}).get("enabled", True) and HAS_AST_PARSER:
            self._parse_view_definitions()
            self._parse_routine_definitions()

        # Extract Composer DAGs if configured
        if extraction_config.get("composer_dags", {}).get("enabled", False):
            self._extract_composer_dags()

        return self._build_cache()

    def _extract_tables(self, dataset: str) -> None:
        """Extract table objects from a dataset."""
        print(f"  Extracting tables from {dataset}...")

        query = f"""
        SELECT
            table_catalog as project_id,
            table_schema as dataset_id,
            table_name,
            table_type,
            creation_time
        FROM `{self.client.project}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        """

        try:
            result = self.client.query(query).result()
            count = 0

            for row in result:
                object_id = self._make_object_id(
                    row.project_id, row.dataset_id, row.table_name
                )

                self.objects[object_id] = {
                    "id": object_id,
                    "schema": f"{row.project_id}.{row.dataset_id}",
                    "name": row.table_name,
                    "type": "BIGQUERY_TABLE",
                    "platform": "bigquery",
                    "owner": "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": row.creation_time.isoformat() if row.creation_time else None,
                    "description": None,
                    "columns": [],  # Skip columns for now
                    # row_count and size_bytes not available in INFORMATION_SCHEMA.TABLES
                }
                count += 1

            print(f"    Found {count} tables")

        except Exception as e:
            print(f"    Error extracting tables: {e}")

    def _extract_views(self, dataset: str) -> None:
        """Extract view objects with definitions."""
        print(f"  Extracting views from {dataset}...")

        query = f"""
        SELECT
            table_catalog as project_id,
            table_schema as dataset_id,
            table_name,
            view_definition
        FROM `{self.client.project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
        """

        try:
            result = self.client.query(query).result()
            count = 0

            for row in result:
                object_id = self._make_object_id(
                    row.project_id, row.dataset_id, row.table_name
                )

                self.objects[object_id] = {
                    "id": object_id,
                    "schema": f"{row.project_id}.{row.dataset_id}",
                    "name": row.table_name,
                    "type": "BIGQUERY_VIEW",
                    "platform": "bigquery",
                    "owner": "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": None,
                    "description": None,
                    "definition": row.view_definition,
                    "columns": [],
                }
                count += 1

            print(f"    Found {count} views")

        except Exception as e:
            print(f"    Error extracting views: {e}")

    def _extract_routines(self, dataset: str) -> None:
        """Extract UDFs and stored procedures."""
        print(f"  Extracting routines from {dataset}...")

        query = f"""
        SELECT
            routine_catalog as project_id,
            routine_schema as dataset_id,
            routine_name,
            routine_type,
            routine_definition,
            created,
            last_altered
        FROM `{self.client.project}.{dataset}.INFORMATION_SCHEMA.ROUTINES`
        """

        try:
            result = self.client.query(query).result()
            count = 0

            for row in result:
                object_id = self._make_object_id(
                    row.project_id, row.dataset_id, row.routine_name
                )

                # Map routine type
                obj_type = "BIGQUERY_UDF" if row.routine_type == "FUNCTION" else "BIGQUERY_PROCEDURE"

                self.objects[object_id] = {
                    "id": object_id,
                    "schema": f"{row.project_id}.{row.dataset_id}",
                    "name": row.routine_name,
                    "type": obj_type,
                    "platform": "bigquery",
                    "owner": "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": row.created.isoformat() if row.created else None,
                    "description": None,
                    "definition": row.routine_definition,
                }
                count += 1

            print(f"    Found {count} routines")

        except Exception as e:
            print(f"    Error extracting routines: {e}")

    def _parse_view_definitions(self) -> None:
        """Parse view definitions to extract table dependencies."""
        print("\nParsing view definitions for dependencies...")

        # Use require_schema=True to filter out unqualified names (functions, variables, keywords)
        sql_parser = SQLParser(dialect="bigquery", require_schema=True)
        deps_found = 0

        views = [obj for obj in self.objects.values() if obj.get("definition")]

        for view in views:
            definition = view.get("definition", "")
            if not definition:
                continue

            try:
                refs = sql_parser.parse(definition)

                for ref in refs:
                    # ref is a TableReference object with .full_id() and .reference_type
                    ref_table = ref.full_id().upper()

                    # Handle different reference formats
                    # Could be: table, dataset.table, project.dataset.table
                    source_id = self._resolve_table_reference(ref_table, view)

                    if source_id and source_id in self.objects:
                        self.table_deps.append({
                            "source_id": source_id,
                            "target_id": view["id"],
                            "dependency_type": "USES",
                            "reference_type": ref.reference_type,
                        })
                        deps_found += 1
                    elif source_id:
                        # Create placeholder for external table reference
                        self._create_external_reference(source_id, ref_table)
                        self.table_deps.append({
                            "source_id": source_id,
                            "target_id": view["id"],
                            "dependency_type": "USES",
                            "reference_type": ref.reference_type,
                        })
                        deps_found += 1

            except Exception as e:
                print(f"  Warning: Failed to parse view {view['name']}: {e}")

        print(f"  Found {deps_found} dependencies from view definitions")

    def _resolve_table_reference(self, ref: str, context_obj: dict) -> Optional[str]:
        """Resolve a table reference to a full object ID."""
        parts = ref.replace("`", "").split(".")

        # Extract project and dataset from context
        schema_parts = context_obj["schema"].split(".")
        context_project = schema_parts[0] if len(schema_parts) > 0 else self.client.project
        context_dataset = schema_parts[1] if len(schema_parts) > 1 else None

        if len(parts) == 3:
            # project.dataset.table
            return f"bigquery:{parts[0]}.{parts[1]}.{parts[2]}"
        elif len(parts) == 2:
            # dataset.table - use context project
            return f"bigquery:{context_project}.{parts[0]}.{parts[1]}"
        elif len(parts) == 1 and context_dataset:
            # Just table name - use context project and dataset
            return f"bigquery:{context_project}.{context_dataset}.{parts[0]}"

        return None

    def _create_external_reference(self, object_id: str, name: str) -> None:
        """Create a placeholder object for external references."""
        if object_id not in self.objects:
            parts = object_id.replace("bigquery:", "").split(".")
            schema = ".".join(parts[:-1]) if len(parts) > 1 else "EXTERNAL"
            table_name = parts[-1] if parts else name

            self.objects[object_id] = {
                "id": object_id,
                "schema": schema,
                "name": table_name,
                "type": "BIGQUERY_TABLE",  # Assume table
                "platform": "bigquery",
                "owner": "EXTERNAL",
                "object_id": self._next_id(),
                "created_at": None,
                "description": "External reference (not in extracted datasets)",
                "columns": [],
            }

    def _parse_routine_definitions(self) -> None:
        """Parse stored procedure and UDF definitions to extract table dependencies."""
        print("\nParsing routine definitions for dependencies...")

        # Use require_schema=True to filter out unqualified names (functions, variables, keywords)
        sql_parser = SQLParser(dialect="bigquery", require_schema=True)
        deps_found = 0

        routines = [obj for obj in self.objects.values()
                    if obj.get("type") in ("BIGQUERY_UDF", "BIGQUERY_PROCEDURE") and obj.get("definition")]

        for routine in routines:
            definition = routine.get("definition", "")
            if not definition:
                continue

            try:
                refs = sql_parser.parse(definition)

                for ref in refs:
                    # ref is a TableReference object with .full_id() and .reference_type
                    ref_table = ref.full_id().upper()
                    source_id = self._resolve_table_reference(ref_table, routine)

                    if source_id:
                        ref_type = ref.reference_type

                        # Determine dependency type based on reference
                        if ref_type in ("DDL", "INSERT", "UPDATE", "DELETE", "MERGE"):
                            dep_type = "WRITES"
                        else:
                            dep_type = "READS"

                        # For writes, routine is source, table is target
                        # For reads, table is source, routine is target
                        if dep_type == "WRITES":
                            self.table_deps.append({
                                "source_id": routine["id"],
                                "target_id": source_id,
                                "dependency_type": dep_type,
                                "reference_type": ref_type,
                            })
                        else:
                            self.table_deps.append({
                                "source_id": source_id,
                                "target_id": routine["id"],
                                "dependency_type": "USES",
                                "reference_type": ref_type,
                            })

                        # Create external reference if needed
                        if source_id not in self.objects:
                            self._create_external_reference(source_id, ref_table)

                        deps_found += 1

            except Exception as e:
                print(f"  Warning: Failed to parse routine {routine['name']}: {e}")

        print(f"  Found {deps_found} dependencies from routine definitions")

    def _extract_composer_dags(self) -> None:
        """
        Extract Cloud Composer DAG information.

        This requires either:
        1. Access to the Composer environment's DAG bucket (GCS)
        2. A metadata table containing DAG definitions

        For now, this supports a metadata table approach where DAG info
        is stored in BigQuery.
        """
        print("\nExtracting Composer DAGs...")

        composer_config = self.config.get("extraction", {}).get("composer_dags", {})

        # Option 1: DAG metadata stored in BigQuery table
        metadata_table = composer_config.get("metadata_table")
        if metadata_table:
            self._extract_dags_from_metadata_table(metadata_table)
            return

        # Option 2: Parse DAG files from GCS bucket
        dag_bucket = composer_config.get("gcs_bucket")
        if dag_bucket:
            self._extract_dags_from_gcs(dag_bucket)
            return

        print("  No Composer DAG source configured. Skipping.")

    def _extract_dags_from_metadata_table(self, table_ref: str) -> None:
        """Extract DAG info from a BigQuery metadata table."""
        print(f"  Extracting DAGs from metadata table: {table_ref}")

        # Expected table schema:
        # dag_id, dag_name, schedule, source_tables (JSON array), target_tables (JSON array)
        query = f"""
        SELECT
            dag_id,
            dag_name,
            schedule_interval,
            source_tables,
            target_tables,
            description
        FROM `{table_ref}`
        """

        try:
            # Need to reconnect if we've disconnected
            if not self.client:
                self.connect()

            result = self.client.query(query).result()
            count = 0

            for row in result:
                dag_id = f"composer:{row.dag_id}"

                self.objects[dag_id] = {
                    "id": dag_id,
                    "schema": "COMPOSER",
                    "name": row.dag_name or row.dag_id,
                    "type": "COMPOSER_DAG",
                    "platform": "composer",
                    "owner": "AIRFLOW",
                    "object_id": self._next_id(),
                    "created_at": None,
                    "description": row.description,
                    "schedule": row.schedule_interval,
                }

                # Parse source tables (DAG reads from these)
                source_tables = row.source_tables or []
                if isinstance(source_tables, str):
                    import json as json_lib
                    source_tables = json_lib.loads(source_tables)

                for source in source_tables:
                    source_id = f"bigquery:{source}" if not source.startswith("bigquery:") else source
                    self.table_deps.append({
                        "source_id": source_id,
                        "target_id": dag_id,
                        "dependency_type": "READS",
                        "reference_type": "DAG_INPUT",
                    })
                    if source_id not in self.objects:
                        self._create_external_reference(source_id, source)

                # Parse target tables (DAG writes to these)
                target_tables = row.target_tables or []
                if isinstance(target_tables, str):
                    import json as json_lib
                    target_tables = json_lib.loads(target_tables)

                for target in target_tables:
                    target_id = f"bigquery:{target}" if not target.startswith("bigquery:") else target
                    self.table_deps.append({
                        "source_id": dag_id,
                        "target_id": target_id,
                        "dependency_type": "WRITES",
                        "reference_type": "DAG_OUTPUT",
                    })
                    if target_id not in self.objects:
                        self._create_external_reference(target_id, target)

                count += 1

            print(f"    Found {count} DAGs")

        except Exception as e:
            print(f"    Error extracting DAGs: {e}")

    def _extract_dags_from_gcs(self, bucket_name: str) -> None:
        """
        Extract DAG info by parsing Python DAG files from GCS.

        This is a more complex approach that requires parsing Python AST.
        For now, this is a placeholder for future implementation.
        """
        print(f"  GCS-based DAG extraction from {bucket_name} not yet implemented.")
        print("  Consider using the metadata_table approach instead.")

    def _build_cache(self) -> dict:
        """Build the final cache structure."""
        return {
            "metadata": {
                "source": "bigquery",
                "projects": self.projects_extracted,
                "extracted_at": datetime.now().isoformat(),
                "extractor_version": "1.0.0",
                "object_count": len(self.objects),
                "dependency_count": len(self.table_deps),
            },
            "objects": self.objects,
            "dependencies": {
                "table_level": self.table_deps,
                "column_level": [],  # Not implemented for BigQuery yet
            },
        }


def merge_caches(exasol_cache: dict, bigquery_cache: dict) -> dict:
    """
    Merge Exasol and BigQuery caches into a single unified cache.

    This allows cross-platform lineage visualization.
    """
    merged = {
        "metadata": {
            "source": "multi-platform",
            "platforms": ["exasol", "bigquery"],
            "extracted_at": datetime.now().isoformat(),
            "extractor_version": "1.0.0",
            "object_count": 0,
            "dependency_count": 0,
        },
        "objects": {},
        "dependencies": {
            "table_level": [],
            "column_level": [],
        },
    }

    # Add Exasol objects (prefix with exasol: if not already)
    for obj_id, obj in exasol_cache.get("objects", {}).items():
        new_id = obj_id if obj_id.startswith("exasol:") else f"exasol:{obj_id}"
        obj_copy = obj.copy()
        obj_copy["id"] = new_id
        obj_copy["platform"] = "exasol"
        merged["objects"][new_id] = obj_copy

    # Add BigQuery objects
    for obj_id, obj in bigquery_cache.get("objects", {}).items():
        merged["objects"][obj_id] = obj

    # Merge dependencies (update IDs for Exasol)
    for dep in exasol_cache.get("dependencies", {}).get("table_level", []):
        dep_copy = dep.copy()
        if not dep_copy["source_id"].startswith("exasol:"):
            dep_copy["source_id"] = f"exasol:{dep_copy['source_id']}"
        if not dep_copy["target_id"].startswith("exasol:"):
            dep_copy["target_id"] = f"exasol:{dep_copy['target_id']}"
        merged["dependencies"]["table_level"].append(dep_copy)

    # Add BigQuery dependencies
    merged["dependencies"]["table_level"].extend(
        bigquery_cache.get("dependencies", {}).get("table_level", [])
    )

    # Update counts
    merged["metadata"]["object_count"] = len(merged["objects"])
    merged["metadata"]["dependency_count"] = len(merged["dependencies"]["table_level"])

    return merged


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file."""
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        print("Creating sample configuration...")

        sample_config = {
            "connection": {
                "project_id": "your-gcp-project-id",
                "credentials_file": "path/to/service-account.json",
            },
            "extraction": {
                "datasets": [],  # Empty = all datasets
                "include_datasets": [],
                "exclude_datasets": [],
                "include_routines": True,
            },
            "script_parsing": {
                "enabled": True,
            },
            "output": {
                "file_path": "../data/bigquery_cache.json",
                "pretty_print": True,
            },
        }

        with open(config_path, "w") as f:
            yaml.dump(sample_config, f, default_flow_style=False)

        print(f"Sample config created at {config_path}")
        print("Please update with your BigQuery settings and run again.")
        sys.exit(0)

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Extract lineage from BigQuery")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "bigquery_config.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--merge-with",
        type=Path,
        help="Path to Exasol cache file to merge with",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Override output file path",
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Extract from BigQuery
    print("=" * 60)
    print("BigQuery Lineage Extractor")
    print("=" * 60)

    extractor = BigQueryLineageExtractor(config)
    cache = extractor.extract()

    # Optionally merge with Exasol cache
    if args.merge_with and args.merge_with.exists():
        print(f"\nMerging with Exasol cache: {args.merge_with}")
        with open(args.merge_with, "r") as f:
            exasol_cache = json.load(f)
        cache = merge_caches(exasol_cache, cache)

    # Determine output path
    output_path = args.output or Path(config.get("output", {}).get("file_path", "../data/bigquery_cache.json"))
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write cache
    pretty = config.get("output", {}).get("pretty_print", True)
    with open(output_path, "w") as f:
        json.dump(cache, f, indent=2 if pretty else None, default=str)

    print("\n" + "=" * 60)
    print("Extraction Complete!")
    print("=" * 60)
    print(f"Objects: {cache['metadata']['object_count']}")
    print(f"Dependencies: {cache['metadata']['dependency_count']}")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
