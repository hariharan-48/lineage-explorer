#!/usr/bin/env python3
"""
Extract lineage from GitHub repos.
Scans repos for bigquery/ folder and parses SQL files.

Usage:
    python extract_from_github.py --org YOUR_ORG --output github_lineage.json
    python extract_from_github.py --org YOUR_ORG --output github_lineage.json --merge-with existing_cache.json
    python extract_from_github.py --org YOUR_ORG --repos repo1,repo2,repo3 --output github_lineage.json

Environment:
    GITHUB_TOKEN: Personal access token with repo read access
    GITHUB_API_URL: GitHub API URL (default: https://api.github.com)
"""

import argparse
import base64
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

import requests

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.script_parser import SQLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class GitHubConfig:
    """GitHub Enterprise configuration."""
    api_url: str
    token: str
    org: str
    bigquery_folder: str = "bigquery"
    branch: str = "main"  # or "master"
    verify_ssl: bool = True
    repos: list = None  # If set, only scan these repos

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }


@dataclass
class ExtractedObject:
    """Represents an extracted database object."""
    object_id: str
    name: str
    schema_name: str
    object_type: str
    source_repo: str
    source_file: str
    sql_text: str = ""


@dataclass
class ExtractedDependency:
    """Represents a dependency between objects."""
    source_id: str
    target_id: str
    dependency_type: str = "DATA"


class GitHubLineageExtractor:
    """Extract lineage from GitHub Enterprise repos."""

    def __init__(self, config: GitHubConfig):
        self.config = config
        self.sql_parser = SQLParser(dialect="bigquery", require_schema=True)
        self.objects: dict[str, ExtractedObject] = {}
        self.dependencies: list[ExtractedDependency] = []
        self.stats = {
            "repos_scanned": 0,
            "repos_with_bigquery": 0,
            "sql_files_parsed": 0,
            "objects_found": 0,
            "dependencies_found": 0,
            "errors": 0
        }

    def list_org_repos(self) -> list[dict]:
        """List all repos in the organization."""
        repos = []
        page = 1
        per_page = 100

        while True:
            url = f"{self.config.api_url}/orgs/{self.config.org}/repos"
            params = {"page": page, "per_page": per_page, "type": "all"}

            response = requests.get(url, headers=self.config.headers, params=params, verify=self.config.verify_ssl)
            if response.status_code != 200:
                logger.error(f"Failed to list repos: {response.status_code} - {response.text}")
                break

            batch = response.json()
            if not batch:
                break

            repos.extend(batch)
            logger.info(f"Fetched {len(repos)} repos so far...")
            page += 1

        logger.info(f"Total repos found: {len(repos)}")
        return repos

    def check_bigquery_folder(self, repo_name: str) -> bool:
        """Check if repo has bigquery/ folder."""
        url = f"{self.config.api_url}/repos/{self.config.org}/{repo_name}/contents/{self.config.bigquery_folder}"
        params = {"ref": self.config.branch}

        response = requests.get(url, headers=self.config.headers, params=params, verify=self.config.verify_ssl)
        return response.status_code == 200

    def get_sql_files(self, repo_name: str, path: str = "") -> list[dict]:
        """Recursively get all .sql files from a path."""
        if not path:
            path = self.config.bigquery_folder

        url = f"{self.config.api_url}/repos/{self.config.org}/{repo_name}/contents/{path}"
        params = {"ref": self.config.branch}

        response = requests.get(url, headers=self.config.headers, params=params, verify=self.config.verify_ssl)
        if response.status_code != 200:
            return []

        sql_files = []
        items = response.json()

        if not isinstance(items, list):
            items = [items]

        for item in items:
            if item["type"] == "file" and item["name"].endswith(".sql"):
                sql_files.append(item)
            elif item["type"] == "dir":
                # Recursively get SQL files from subdirectories
                sql_files.extend(self.get_sql_files(repo_name, item["path"]))

        return sql_files

    def get_file_content(self, repo_name: str, file_path: str) -> Optional[str]:
        """Get content of a file."""
        url = f"{self.config.api_url}/repos/{self.config.org}/{repo_name}/contents/{file_path}"
        params = {"ref": self.config.branch}

        response = requests.get(url, headers=self.config.headers, params=params, verify=self.config.verify_ssl)
        if response.status_code != 200:
            return None

        data = response.json()
        if data.get("encoding") == "base64":
            return base64.b64decode(data["content"]).decode("utf-8")
        return data.get("content")

    def extract_target_from_sql(self, sql: str) -> Optional[tuple[str, str, str]]:
        """
        Extract target object from CREATE statement.
        Returns (project_dataset_table, object_type, name) or None.
        """
        # Patterns for CREATE statements
        patterns = [
            # CREATE OR REPLACE VIEW/TABLE project.dataset.name
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(VIEW|TABLE|PROCEDURE|FUNCTION)\s+(?:`?([a-zA-Z0-9_-]+)`?\.)?`?([a-zA-Z0-9_-]+)`?\.`?([a-zA-Z0-9_-]+)`?",
        ]

        sql_upper = sql.upper()

        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE | re.MULTILINE)
            if match:
                obj_type = match.group(1).upper()
                project = match.group(2) or ""
                dataset = match.group(3)
                name = match.group(4)

                # Build full qualified name
                if project:
                    full_name = f"{project}.{dataset}.{name}"
                else:
                    full_name = f"{dataset}.{name}"

                return (full_name, obj_type, name)

        return None

    def parse_sql_file(self, repo_name: str, file_info: dict) -> None:
        """Parse a SQL file and extract lineage."""
        file_path = file_info["path"]

        content = self.get_file_content(repo_name, file_path)
        if not content:
            logger.warning(f"Could not fetch content for {repo_name}/{file_path}")
            self.stats["errors"] += 1
            return

        self.stats["sql_files_parsed"] += 1

        # Extract target from CREATE statement
        target_info = self.extract_target_from_sql(content)
        if not target_info:
            logger.debug(f"No CREATE statement found in {file_path}")
            return

        target_full_name, obj_type, target_name = target_info

        # Parse for source tables
        try:
            table_refs = self.sql_parser.parse(content)
            source_tables = [{"name": ref.name, "schema": ref.schema} for ref in table_refs]
        except Exception as e:
            logger.warning(f"Failed to parse SQL in {file_path}: {e}")
            source_tables = []

        # Determine schema from target
        parts = target_full_name.split(".")
        if len(parts) >= 2:
            schema_name = parts[-2]  # dataset name
        else:
            schema_name = "UNKNOWN"

        # Create target object
        target_id = f"BIGQUERY.{target_full_name}".upper()

        if target_id not in self.objects:
            self.objects[target_id] = ExtractedObject(
                object_id=target_id,
                name=target_name,
                schema_name=schema_name,
                object_type=obj_type,
                source_repo=repo_name,
                source_file=file_path,
                sql_text=content[:1000]  # Store first 1000 chars
            )
            self.stats["objects_found"] += 1

        # Create dependencies
        for source in source_tables:
            source_name = source.get("name", "")
            source_schema = source.get("schema", "")

            if source_schema:
                source_full = f"{source_schema}.{source_name}"
            else:
                source_full = source_name

            source_id = f"BIGQUERY.{source_full}".upper()

            # Create source object if not exists
            if source_id not in self.objects:
                self.objects[source_id] = ExtractedObject(
                    object_id=source_id,
                    name=source_name,
                    schema_name=source_schema or "UNKNOWN",
                    object_type="TABLE",  # Assume table
                    source_repo=repo_name,
                    source_file=file_path
                )
                self.stats["objects_found"] += 1

            # Create dependency
            dep = ExtractedDependency(
                source_id=source_id,
                target_id=target_id,
                dependency_type="DATA"
            )
            self.dependencies.append(dep)
            self.stats["dependencies_found"] += 1

    def process_repo(self, repo: dict) -> None:
        """Process a single repository."""
        repo_name = repo["name"]
        self.stats["repos_scanned"] += 1

        # Check if repo has bigquery folder
        if not self.check_bigquery_folder(repo_name):
            logger.debug(f"Skipping {repo_name} - no bigquery/ folder")
            return

        logger.info(f"Processing {repo_name}...")
        self.stats["repos_with_bigquery"] += 1

        # Get all SQL files
        sql_files = self.get_sql_files(repo_name)
        logger.info(f"  Found {len(sql_files)} SQL files")

        # Parse each SQL file
        for file_info in sql_files:
            self.parse_sql_file(repo_name, file_info)

    def run(self) -> dict:
        """Run the extraction process."""
        logger.info(f"Starting extraction from {self.config.org}...")

        # Use specified repos or list all repos
        if self.config.repos:
            logger.info(f"Scanning specified repos: {self.config.repos}")
            repos = [{"name": name} for name in self.config.repos]
        else:
            repos = self.list_org_repos()

        # Process each repo
        for repo in repos:
            try:
                self.process_repo(repo)
            except Exception as e:
                logger.error(f"Error processing {repo['name']}: {e}")
                self.stats["errors"] += 1

        logger.info(f"Extraction complete. Stats: {self.stats}")
        return self.build_cache()

    def build_cache(self) -> dict:
        """Build the lineage cache structure."""
        objects_list = []
        for obj in self.objects.values():
            objects_list.append({
                "id": obj.object_id,  # String ID like "SCHEMA.TABLE"
                "object_id": hash(obj.object_id) % 10000000,  # Numeric ID for Pydantic
                "name": obj.name,
                "schema": obj.schema_name,
                "type": obj.object_type,
                "platform": "bigquery",
                "owner": obj.schema_name or "BIGQUERY",
                "database": "BIGQUERY",
                "source_repo": obj.source_repo,
                "source_file": obj.source_file
            })

        deps_list = []
        for dep in self.dependencies:
            deps_list.append({
                "source_id": dep.source_id,
                "target_id": dep.target_id,
                "source_object_id": dep.source_id,  # Keep for backwards compat
                "target_object_id": dep.target_id,
                "dependency_type": dep.dependency_type
            })

        return {
            "metadata": {
                "source": "github",
                "organization": self.config.org,
                "extracted_at": datetime.now().isoformat(),
                "stats": self.stats
            },
            "objects": objects_list,
            "dependencies": deps_list
        }


def merge_caches(base: dict, new: dict) -> dict:
    """Merge two lineage caches."""
    # Merge objects (avoid duplicates by id)
    existing_ids = {obj.get("id") or obj.get("object_id") for obj in base.get("objects", [])}
    for obj in new.get("objects", []):
        obj_id = obj.get("id") or obj.get("object_id")
        if obj_id not in existing_ids:
            base["objects"].append(obj)
            existing_ids.add(obj_id)

    # Merge dependencies (avoid duplicates)
    def get_dep_key(d):
        src = d.get("source_id") or d.get("source_object_id")
        tgt = d.get("target_id") or d.get("target_object_id")
        return (src, tgt)

    existing_deps = {get_dep_key(d) for d in base.get("dependencies", [])}
    for dep in new.get("dependencies", []):
        key = get_dep_key(dep)
        if key not in existing_deps:
            base["dependencies"].append(dep)
            existing_deps.add(key)

    # Update metadata
    base["metadata"]["merged_at"] = datetime.now().isoformat()
    base["metadata"]["github_stats"] = new["metadata"].get("stats", {})

    return base


def main():
    parser = argparse.ArgumentParser(description="Extract lineage from GitHub Enterprise repos")
    parser.add_argument("--org", required=True, help="GitHub organization name")
    parser.add_argument("--output", default="github_lineage.json", help="Output file path")
    parser.add_argument("--merge-with", help="Existing cache file to merge with")
    parser.add_argument("--api-url", help="GitHub API URL (or set GITHUB_API_URL env var)")
    parser.add_argument("--token", help="GitHub token (or set GITHUB_TOKEN env var)")
    parser.add_argument("--bigquery-folder", default="bigquery", help="BigQuery folder name")
    parser.add_argument("--branch", default="main", help="Branch to scan (default: main)")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")
    parser.add_argument("--repos", help="Comma-separated list of repo names to scan (default: scan all)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get config from args or env
    api_url = args.api_url or os.environ.get("GITHUB_API_URL", "https://api.github.com")
    token = args.token or os.environ.get("GITHUB_TOKEN")

    if not token:
        logger.error("GitHub token required. Set GITHUB_TOKEN env var or use --token")
        sys.exit(1)

    # Parse repos list if provided
    repos_list = None
    if args.repos:
        repos_list = [r.strip() for r in args.repos.split(",")]

    config = GitHubConfig(
        api_url=api_url,
        token=token,
        org=args.org,
        bigquery_folder=args.bigquery_folder,
        branch=args.branch,
        verify_ssl=not args.no_verify_ssl,
        repos=repos_list
    )

    # Run extraction
    extractor = GitHubLineageExtractor(config)
    cache = extractor.run()

    # Merge if requested
    if args.merge_with:
        if Path(args.merge_with).exists():
            with open(args.merge_with) as f:
                base_cache = json.load(f)
            cache = merge_caches(base_cache, cache)
            logger.info(f"Merged with {args.merge_with}")
        else:
            logger.warning(f"Merge file not found: {args.merge_with}")

    # Save output
    output_path = Path(args.output)
    with open(output_path, "w") as f:
        json.dump(cache, f, indent=2)

    logger.info(f"Saved to {output_path}")
    logger.info(f"Objects: {len(cache['objects'])}, Dependencies: {len(cache['dependencies'])}")


if __name__ == "__main__":
    main()
