#!/usr/bin/env python3
"""
Exasol Lineage Extractor
========================
Extracts database metadata and dependencies from an Exasol database
to build the lineage cache. This script is plug-and-play configurable
for any Exasol environment.

Usage:
    python extract_from_exasol.py [--config path/to/config.yaml]

Requirements:
    pip install pyexasol pyyaml
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
    import pyexasol
except ImportError:
    print("pyexasol not installed. Run: pip install pyexasol")
    print("Note: pyexasol requires Exasol ODBC driver or websocket connection")
    sys.exit(1)

# Import AST-based parser
try:
    from script_parser import parse_script, SQLParser
    HAS_AST_PARSER = True
except ImportError:
    HAS_AST_PARSER = False
    print("Warning: script_parser not found. Script parsing will be limited.")


class ExasolLineageExtractor:
    """
    Extracts lineage data from Exasol database using system tables:
    - EXA_ALL_TABLES / EXA_ALL_COLUMNS
    - EXA_ALL_VIEWS / EXA_ALL_VIEW_COLUMNS
    - EXA_ALL_SCRIPTS
    - EXA_ALL_VIRTUAL_SCHEMAS
    - EXA_ALL_CONNECTIONS
    - EXA_DBA_DEPENDENCIES
    """

    def __init__(self, config: dict):
        self.config = config
        self.conn = None
        self.objects: Dict[str, dict] = {}
        self.table_deps: List[dict] = []
        self.column_deps: List[dict] = []
        self.object_counter = 100000

    def connect(self) -> None:
        """Establish connection to Exasol database."""
        conn_config = self.config["connection"]

        # Get password from config or environment variable
        password = conn_config.get("password")
        if not password and "password_env" in conn_config:
            password = os.environ.get(conn_config["password_env"])
            if not password:
                raise ValueError(
                    f"Environment variable {conn_config['password_env']} not set"
                )

        dsn = f"{conn_config['host']}:{conn_config['port']}"

        print(f"Connecting to Exasol at {dsn}...")
        self.conn = pyexasol.connect(
            dsn=dsn,
            user=conn_config["user"],
            password=password,
            schema=conn_config.get("schema", ""),
        )
        print("Connected successfully!")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("Disconnected from Exasol.")

    def extract(self) -> dict:
        """Main extraction process."""
        try:
            self.connect()

            extraction_config = self.config.get("extraction", {})
            object_types = extraction_config.get("object_types", {})

            # Extract each object type
            if object_types.get("connections", True):
                self._extract_connections()

            if object_types.get("virtual_schemas", True):
                self._extract_virtual_schemas()

            if object_types.get("tables", True):
                self._extract_tables()

            if object_types.get("views", True):
                self._extract_views()

            if object_types.get("lua_udfs", True):
                self._extract_scripts()

            # Extract scripts from custom metadata tables
            self._extract_metadata_scripts()

            # Parse view definitions for dependencies (using AST parser)
            if self.config.get("script_parsing", {}).get("enabled", True):
                self._parse_view_definitions()

            # Extract dependencies from system table
            self._extract_dependencies()

            # Extract column-level lineage if enabled
            if extraction_config.get("extract_column_lineage", True):
                self._extract_column_lineage()

            return self._build_cache()

        finally:
            self.disconnect()

    def _should_include_schema(self, schema: str) -> bool:
        """Check if schema should be included based on config."""
        extraction = self.config.get("extraction", {})
        include = extraction.get("include_schemas", [])
        exclude = extraction.get("exclude_schemas", [])

        if include and schema not in include:
            return False
        if schema in exclude:
            return False
        return True

    def _extract_connections(self) -> None:
        """Extract connection objects."""
        print("Extracting connections...")

        query = """
        SELECT
            CONNECTION_NAME,
            CONNECTION_STRING,
            USER_NAME,
            CREATED,
            CONNECTION_COMMENT
        FROM EXA_DBA_CONNECTIONS
        """

        try:
            result = self.conn.execute(query)
            for row in result:
                conn_name = row[0]
                conn_id = f"SYS.{conn_name}"

                self.objects[conn_id] = {
                    "id": conn_id,
                    "schema": "SYS",
                    "name": conn_name,
                    "type": "CONNECTION",
                    "owner": "SYS",
                    "object_id": self._next_id(),
                    "created_at": row[3].isoformat() if row[3] else datetime.now().isoformat(),
                    "description": row[4] or f"Connection {conn_name}",
                    "connection_string": row[1] or "",
                    "user": row[2] or "",
                }

            print(f"  Found {len([o for o in self.objects.values() if o['type'] == 'CONNECTION'])} connections")
        except Exception as e:
            print(f"  Warning: Could not extract connections: {e}")

    def _extract_virtual_schemas(self) -> None:
        """Extract virtual schema objects."""
        print("Extracting virtual schemas...")

        query = """
        SELECT
            SCHEMA_NAME,
            SCHEMA_OWNER,
            ADAPTER_SCRIPT_SCHEMA,
            ADAPTER_SCRIPT_NAME,
            ADAPTER_NOTES,
            CREATED
        FROM EXA_ALL_VIRTUAL_SCHEMAS
        """

        try:
            result = self.conn.execute(query)
            for row in result:
                schema_name = row[0]
                if not self._should_include_schema(schema_name):
                    continue

                vs_id = f"{schema_name}.VIRTUAL_SCHEMA"

                self.objects[vs_id] = {
                    "id": vs_id,
                    "schema": schema_name,
                    "name": "VIRTUAL_SCHEMA",
                    "type": "VIRTUAL_SCHEMA",
                    "owner": row[1] or "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": row[5].isoformat() if row[5] else datetime.now().isoformat(),
                    "description": row[4] or f"Virtual schema {schema_name}",
                    "adapter_name": f"{row[2]}.{row[3]}" if row[2] and row[3] else None,
                }

            print(f"  Found {len([o for o in self.objects.values() if o['type'] == 'VIRTUAL_SCHEMA'])} virtual schemas")
        except Exception as e:
            print(f"  Warning: Could not extract virtual schemas: {e}")

    def _extract_tables(self) -> None:
        """Extract table objects with columns."""
        print("Extracting tables...")

        # Get tables
        table_query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_OWNER,
            TABLE_COMMENT,
            TABLE_ROW_COUNT,
            RAW_OBJECT_SIZE,
            CREATED
        FROM EXA_ALL_TABLES
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """

        tables_by_schema: Dict[str, List] = {}

        result = self.conn.execute(table_query)
        for row in result:
            schema_name = row[0]
            if not self._should_include_schema(schema_name):
                continue

            if schema_name not in tables_by_schema:
                tables_by_schema[schema_name] = []
            tables_by_schema[schema_name].append(row)

        # Get columns for all tables at once
        column_query = """
        SELECT
            COLUMN_SCHEMA,
            COLUMN_TABLE,
            COLUMN_NAME,
            COLUMN_TYPE,
            COLUMN_ORDINAL_POSITION,
            COLUMN_IS_NULLABLE,
            COLUMN_COMMENT
        FROM EXA_ALL_COLUMNS
        WHERE COLUMN_OBJECT_TYPE = 'TABLE'
        ORDER BY COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_ORDINAL_POSITION
        """

        columns_map: Dict[str, List[dict]] = {}
        result = self.conn.execute(column_query)
        for row in result:
            key = f"{row[0]}.{row[1]}"
            if key not in columns_map:
                columns_map[key] = []
            columns_map[key].append({
                "name": row[2],
                "data_type": row[3],
                "ordinal_position": row[4],
                "is_nullable": row[5],
                "is_primary_key": False,  # Would need to query constraints
                "description": row[6],
            })

        # Build table objects
        for schema_name, tables in tables_by_schema.items():
            for row in tables:
                table_name = row[1]
                table_id = f"{schema_name}.{table_name}"

                self.objects[table_id] = {
                    "id": table_id,
                    "schema": schema_name,
                    "name": table_name,
                    "type": "TABLE",
                    "owner": row[2] or "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": row[6].isoformat() if row[6] else datetime.now().isoformat(),
                    "description": row[3],
                    "columns": columns_map.get(table_id, []),
                    "row_count": row[4],
                    "size_bytes": row[5],
                }

        print(f"  Found {len([o for o in self.objects.values() if o['type'] == 'TABLE'])} tables")

    def _extract_views(self) -> None:
        """Extract view objects with columns and definitions."""
        print("Extracting views...")

        view_query = """
        SELECT
            VIEW_SCHEMA,
            VIEW_NAME,
            VIEW_OWNER,
            VIEW_TEXT,
            VIEW_COMMENT,
            CREATED
        FROM EXA_ALL_VIEWS
        ORDER BY VIEW_SCHEMA, VIEW_NAME
        """

        views_by_schema: Dict[str, List] = {}

        result = self.conn.execute(view_query)
        for row in result:
            schema_name = row[0]
            if not self._should_include_schema(schema_name):
                continue

            if schema_name not in views_by_schema:
                views_by_schema[schema_name] = []
            views_by_schema[schema_name].append(row)

        # Get columns for all views
        column_query = """
        SELECT
            COLUMN_SCHEMA,
            COLUMN_TABLE,
            COLUMN_NAME,
            COLUMN_TYPE,
            COLUMN_ORDINAL_POSITION,
            COLUMN_IS_NULLABLE,
            COLUMN_COMMENT
        FROM EXA_ALL_COLUMNS
        WHERE COLUMN_OBJECT_TYPE = 'VIEW'
        ORDER BY COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_ORDINAL_POSITION
        """

        columns_map: Dict[str, List[dict]] = {}
        result = self.conn.execute(column_query)
        for row in result:
            key = f"{row[0]}.{row[1]}"
            if key not in columns_map:
                columns_map[key] = []
            columns_map[key].append({
                "name": row[2],
                "data_type": row[3],
                "ordinal_position": row[4],
                "is_nullable": row[5],
                "is_primary_key": False,
                "description": row[6],
                "source_columns": [],
            })

        # Build view objects
        for schema_name, views in views_by_schema.items():
            for row in views:
                view_name = row[1]
                view_id = f"{schema_name}.{view_name}"

                self.objects[view_id] = {
                    "id": view_id,
                    "schema": schema_name,
                    "name": view_name,
                    "type": "VIEW",
                    "owner": row[2] or "UNKNOWN",
                    "object_id": self._next_id(),
                    "created_at": row[5].isoformat() if row[5] else datetime.now().isoformat(),
                    "description": row[4],
                    "definition": row[3],
                    "columns": columns_map.get(view_id, []),
                }

        print(f"  Found {len([o for o in self.objects.values() if o['type'] == 'VIEW'])} views")

    def _extract_scripts(self) -> None:
        """Extract Lua UDF scripts."""
        print("Extracting Lua scripts...")

        script_query = """
        SELECT
            SCRIPT_SCHEMA,
            SCRIPT_NAME,
            SCRIPT_OWNER,
            SCRIPT_TYPE,
            SCRIPT_INPUT_TYPE,
            SCRIPT_RESULT_TYPE,
            SCRIPT_TEXT,
            SCRIPT_COMMENT,
            SCRIPT_LANGUAGE,
            CREATED
        FROM EXA_ALL_SCRIPTS
        WHERE SCRIPT_LANGUAGE = 'LUA' OR SCRIPT_LANGUAGE = 'PYTHON'
        ORDER BY SCRIPT_SCHEMA, SCRIPT_NAME
        """

        result = self.conn.execute(script_query)
        for row in result:
            schema_name = row[0]
            if not self._should_include_schema(schema_name):
                continue

            script_name = row[1]
            script_id = f"{schema_name}.{script_name}"

            # Determine UDF type
            script_type = row[3]  # SCALAR, SET, etc.
            language = row[8] or "LUA"

            self.objects[script_id] = {
                "id": script_id,
                "schema": schema_name,
                "name": script_name,
                "type": "LUA_UDF",
                "owner": row[2] or "UNKNOWN",
                "object_id": self._next_id(),
                "created_at": row[9].isoformat() if row[9] else datetime.now().isoformat(),
                "description": row[7],
                "udf_type": script_type,
                "script_language": language,
                "script_text": row[6],
                "input_parameters": self._parse_input_type(row[4]),
                "output_columns": self._parse_result_type(row[5]),
            }

            # Parse script for table references using AST parser
            if self.config.get("script_parsing", {}).get("enabled", True):
                self._parse_script_dependencies(script_id, row[6], language)

        print(f"  Found {len([o for o in self.objects.values() if o['type'] == 'LUA_UDF'])} scripts")

    def _extract_metadata_scripts(self) -> None:
        """
        Extract scripts from custom metadata tables.

        Many organizations store scripts, SQL transformations, or ETL logic in
        custom metadata tables. This method allows configurable extraction from
        such tables.

        Config example:
            metadata_tables:
              - table: "META.LUA_SCRIPTS"
                id_column: "SCRIPT_ID"
                name_column: "SCRIPT_NAME"
                schema_column: "SCHEMA_NAME"  # Optional
                script_column: "SCRIPT_TEXT"
                language: "LUA"  # LUA, PYTHON, or SQL
              - table: "ETL.SQL_TRANSFORMATIONS"
                id_column: "TRANSFORM_ID"
                name_column: "TRANSFORM_NAME"
                script_columns:  # Multiple columns containing SQL parts
                  - "SOURCE_SQL"
                  - "TRANSFORM_SQL"
                  - "TARGET_SQL"
                language: "SQL"
        """
        metadata_config = self.config.get("metadata_tables", [])

        if not metadata_config:
            return

        print("Extracting scripts from metadata tables...")

        for table_config in metadata_config:
            table_name = table_config.get("table")
            if not table_name:
                continue

            print(f"  Processing {table_name}...")

            try:
                # Build SELECT query
                columns = [
                    table_config.get("id_column", "ID"),
                    table_config.get("name_column", "NAME"),
                ]

                schema_column = table_config.get("schema_column")
                if schema_column:
                    columns.append(schema_column)

                # Handle single script column or multiple columns
                script_column = table_config.get("script_column")
                script_columns = table_config.get("script_columns", [])

                if script_column:
                    columns.append(script_column)
                elif script_columns:
                    columns.extend(script_columns)
                else:
                    continue

                # Execute query
                query = f"SELECT {', '.join(columns)} FROM {table_name}"
                where_clause = table_config.get("where_clause")
                if where_clause:
                    query += f" WHERE {where_clause}"

                result = self.conn.execute(query)
                language = table_config.get("language", "SQL").upper()
                script_type = table_config.get("type", "METADATA_SCRIPT")
                count = 0

                for row in result:
                    row_id = str(row[0])
                    row_name = str(row[1])

                    # Determine schema
                    if schema_column:
                        row_schema = str(row[2]).upper()
                        script_start_idx = 3
                    else:
                        row_schema = table_config.get("default_schema", "METADATA")
                        script_start_idx = 2

                    # Combine script columns into single text
                    if script_column:
                        combined_script = row[script_start_idx] or ""
                    else:
                        # Multiple columns - combine them
                        script_parts = row[script_start_idx:]
                        combined_script = "\n".join(str(p) for p in script_parts if p)

                    if not combined_script:
                        continue

                    # Create object ID
                    script_id = f"{row_schema}.{row_name}"

                    # Add as object
                    self.objects[script_id] = {
                        "id": script_id,
                        "schema": row_schema,
                        "name": row_name,
                        "type": "LUA_UDF" if language == "LUA" else "VIEW",  # Treat SQL as view-like
                        "owner": "METADATA",
                        "object_id": self._next_id(),
                        "created_at": datetime.now().isoformat(),
                        "description": f"Script from {table_name}",
                        "script_language": language,
                        "script_text": combined_script,
                        "source_table": table_name,
                        "source_id": row_id,
                    }

                    # Parse script for dependencies
                    self._parse_script_dependencies(script_id, combined_script, language)
                    count += 1

                print(f"    Extracted {count} scripts from {table_name}")

            except Exception as e:
                print(f"    Warning: Could not extract from {table_name}: {e}")

    def _parse_input_type(self, input_type: str) -> List[dict]:
        """Parse script input type string into parameter list."""
        if not input_type:
            return []
        # Simple parsing - could be enhanced
        params = []
        for i, param in enumerate(input_type.split(",")):
            param = param.strip()
            if param:
                params.append({"name": f"param_{i+1}", "data_type": param})
        return params

    def _parse_result_type(self, result_type: str) -> List[dict]:
        """Parse script result type string into column list."""
        if not result_type:
            return []
        columns = []
        for i, col in enumerate(result_type.split(",")):
            col = col.strip()
            if col:
                columns.append({"name": f"col_{i+1}", "data_type": col})
        return columns

    def _parse_script_dependencies(self, script_id: str, script_text: str, language: str = "LUA") -> None:
        """
        Parse script to find table references using AST-based parser.

        Handles:
        - SELECT, INSERT, UPDATE, DELETE, MERGE statements
        - JOINs (INNER, LEFT, RIGHT, FULL)
        - CTEs (WITH clauses) - excludes CTE names from references
        - Subqueries
        - Self-references (script reading from table it writes to)
        """
        if not script_text:
            return

        # Use AST-based parser if available
        if HAS_AST_PARSER:
            known_objects = set(self.objects.keys())
            refs = parse_script(script_text, language, known_objects)

            for ref in refs:
                table_id = ref.full_id()

                # Validate the reference exists in our objects
                if table_id not in self.objects:
                    # Try to find by name only
                    for obj_id in self.objects:
                        if obj_id.endswith(f".{ref.name}"):
                            table_id = obj_id
                            break
                    else:
                        continue  # Skip unknown objects

                # Map reference type to dependency type
                if ref.reference_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE'):
                    # Script writes to this table (target is the table)
                    self.table_deps.append({
                        "source_id": script_id,
                        "target_id": table_id,
                        "dependency_type": "UDF_OUTPUT",
                        "reference_type": ref.reference_type,
                    })
                else:
                    # Script reads from this table (source is the table)
                    self.table_deps.append({
                        "source_id": table_id,
                        "target_id": script_id,
                        "dependency_type": "UDF_INPUT",
                        "reference_type": ref.reference_type,
                    })
        else:
            # Fallback to basic regex (limited functionality)
            self._parse_script_dependencies_fallback(script_id, script_text)

    def _parse_script_dependencies_fallback(self, script_id: str, script_text: str) -> None:
        """Fallback regex-based parsing when AST parser is not available."""
        import re

        patterns = [
            (r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'SELECT'),
            (r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'JOIN'),
            (r'\bINTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'INSERT'),
            (r'\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'UPDATE'),
            (r'\bMERGE\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'MERGE'),
            (r'\bTRUNCATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'TRUNCATE'),
            (r'\bDELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'DELETE'),
        ]

        found_refs: Set[tuple] = set()

        for pattern, ref_type in patterns:
            try:
                matches = re.findall(pattern, script_text, re.IGNORECASE)
                for match in matches:
                    table_ref = match.strip().upper()
                    if "." not in table_ref:
                        for obj_id in self.objects:
                            if obj_id.endswith(f".{table_ref}"):
                                found_refs.add((obj_id, ref_type))
                                break
                    elif table_ref in self.objects:
                        found_refs.add((table_ref, ref_type))
            except Exception:
                continue

        for table_id, ref_type in found_refs:
            if ref_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE'):
                self.table_deps.append({
                    "source_id": script_id,
                    "target_id": table_id,
                    "dependency_type": "UDF_OUTPUT",
                    "reference_type": ref_type,
                })
            else:
                self.table_deps.append({
                    "source_id": table_id,
                    "target_id": script_id,
                    "dependency_type": "UDF_INPUT",
                    "reference_type": ref_type,
                })

    def _parse_view_definitions(self) -> None:
        """
        Parse view definitions to extract table dependencies using AST parser.

        This supplements the EXA_DBA_DEPENDENCIES system table with more detailed
        information about how views reference tables (JOIN, subquery, CTE, etc.)
        """
        print("Parsing view definitions for dependencies...")

        views = [o for o in self.objects.values() if o["type"] == "VIEW"]
        dep_count = 0

        for view in views:
            definition = view.get("definition", "")
            if not definition:
                continue

            view_id = view["id"]

            # Use AST parser to find table references
            if HAS_AST_PARSER:
                known_objects = set(self.objects.keys())
                refs = parse_script(definition, "SQL", known_objects)

                for ref in refs:
                    table_id = ref.full_id()

                    # Validate the reference exists
                    if table_id not in self.objects:
                        for obj_id in self.objects:
                            if obj_id.endswith(f".{ref.name}"):
                                table_id = obj_id
                                break
                        else:
                            continue

                    # Skip self-references
                    if table_id == view_id:
                        continue

                    # Check if this dependency already exists
                    existing = any(
                        d["source_id"] == table_id and d["target_id"] == view_id
                        for d in self.table_deps
                    )

                    if not existing:
                        self.table_deps.append({
                            "source_id": table_id,
                            "target_id": view_id,
                            "dependency_type": "VIEW",
                            "reference_type": ref.reference_type,
                        })
                        dep_count += 1

        print(f"  Found {dep_count} additional dependencies from view definitions")

    def _extract_dependencies(self) -> None:
        """Extract dependencies from EXA_DBA_DEPENDENCIES system table."""
        print("Extracting dependencies...")

        # Exasol's dependency table shows what objects reference what
        dep_query = """
        SELECT
            REFERENCED_OBJECT_SCHEMA,
            REFERENCED_OBJECT_NAME,
            REFERENCED_OBJECT_TYPE,
            OBJECT_SCHEMA,
            OBJECT_NAME,
            OBJECT_TYPE,
            DEPENDENCY_TYPE
        FROM EXA_DBA_DEPENDENCIES
        """

        try:
            result = self.conn.execute(dep_query)
            for row in result:
                ref_schema = row[0]
                ref_name = row[1]
                ref_type = row[2]
                obj_schema = row[3]
                obj_name = row[4]
                obj_type = row[5]
                dep_type = row[6]

                # Build object IDs
                source_id = f"{ref_schema}.{ref_name}"
                target_id = f"{obj_schema}.{obj_name}"

                # Only add if both objects exist in our extracted data
                if source_id in self.objects and target_id in self.objects:
                    # Determine dependency type
                    if obj_type == "VIEW":
                        dependency_type = "VIEW"
                        reference_type = "SELECT"
                    elif "SCRIPT" in obj_type:
                        dependency_type = "UDF_INPUT"
                        reference_type = "PARAMETER"
                    else:
                        dependency_type = "ETL"
                        reference_type = "REFERENCE"

                    self.table_deps.append({
                        "source_id": source_id,
                        "target_id": target_id,
                        "dependency_type": dependency_type,
                        "reference_type": reference_type,
                    })

            print(f"  Found {len(self.table_deps)} dependencies")
        except Exception as e:
            print(f"  Warning: Could not extract dependencies: {e}")

    def _extract_column_lineage(self) -> None:
        """
        Extract column-level lineage by parsing view definitions.
        This is a simplified parser - production use may need more sophisticated SQL parsing.
        """
        print("Extracting column-level lineage...")

        views = [o for o in self.objects.values() if o["type"] == "VIEW"]
        column_deps_count = 0

        for view in views:
            definition = view.get("definition", "")
            if not definition:
                continue

            view_id = view["id"]
            view_columns = view.get("columns", [])

            # Find source tables from dependencies
            source_tables = [
                dep["source_id"]
                for dep in self.table_deps
                if dep["target_id"] == view_id
            ]

            # Simple column mapping - match by name
            for source_id in source_tables:
                source_obj = self.objects.get(source_id)
                if not source_obj or "columns" not in source_obj:
                    continue

                source_columns = source_obj["columns"]

                for view_col in view_columns:
                    view_col_name = view_col["name"].upper()

                    for source_col in source_columns:
                        source_col_name = source_col["name"].upper()

                        # Check if column names match or are referenced in definition
                        if view_col_name == source_col_name or source_col_name in definition.upper():
                            self.column_deps.append({
                                "source_object_id": source_id,
                                "source_column": source_col["name"],
                                "target_object_id": view_id,
                                "target_column": view_col["name"],
                                "transformation": None,
                            })
                            column_deps_count += 1

        print(f"  Found {column_deps_count} column-level dependencies")

    def _next_id(self) -> int:
        """Generate unique object ID."""
        self.object_counter += 1
        return self.object_counter

    def _build_cache(self) -> dict:
        """Build the final cache structure."""
        # Build indexes
        by_schema: Dict[str, List[str]] = {}
        by_type: Dict[str, List[str]] = {}
        forward_edges: Dict[str, List[str]] = {}
        backward_edges: Dict[str, List[str]] = {}

        for obj_id, obj in self.objects.items():
            schema = obj["schema"]
            obj_type = obj["type"]

            if schema not in by_schema:
                by_schema[schema] = []
            by_schema[schema].append(obj_id)

            if obj_type not in by_type:
                by_type[obj_type] = []
            by_type[obj_type].append(obj_id)

        for dep in self.table_deps:
            src, tgt = dep["source_id"], dep["target_id"]

            if src not in forward_edges:
                forward_edges[src] = []
            if tgt not in forward_edges[src]:
                forward_edges[src].append(tgt)

            if tgt not in backward_edges:
                backward_edges[tgt] = []
            if src not in backward_edges[tgt]:
                backward_edges[tgt].append(src)

        return {
            "metadata": {
                "version": "1.0.0",
                "generated_at": datetime.now().isoformat(),
                "source_database": self.config["connection"]["host"],
                "object_count": len(self.objects),
                "column_count": sum(
                    len(obj.get("columns", [])) for obj in self.objects.values()
                ),
                "dependency_count": len(self.table_deps),
            },
            "objects": self.objects,
            "dependencies": {
                "table_level": self.table_deps,
                "column_level": self.column_deps,
            },
            "indexes": {
                "by_schema": by_schema,
                "by_type": by_type,
                "forward_edges": forward_edges,
                "backward_edges": backward_edges,
            },
        }


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    path = Path(config_path)

    # Try local config first
    local_path = path.with_suffix(".local.yaml")
    if local_path.exists():
        print(f"Using local config: {local_path}")
        path = local_path

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Extract lineage data from Exasol database"
    )
    parser.add_argument(
        "--config",
        default="exasol_config.yaml",
        help="Path to configuration file (default: exasol_config.yaml)",
    )
    parser.add_argument(
        "--output",
        help="Override output file path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config without connecting to database",
    )

    args = parser.parse_args()

    # Load configuration
    print(f"Loading configuration from {args.config}...")
    config = load_config(args.config)

    if args.dry_run:
        print("Dry run - configuration is valid!")
        print(f"  Host: {config['connection']['host']}")
        print(f"  Include schemas: {config.get('extraction', {}).get('include_schemas', 'ALL')}")
        print(f"  Exclude schemas: {config.get('extraction', {}).get('exclude_schemas', [])}")
        return

    # Extract lineage
    extractor = ExasolLineageExtractor(config)
    cache = extractor.extract()

    # Determine output path
    output_path = args.output or config.get("output", {}).get(
        "file_path", "../data/lineage_cache.json"
    )
    output_path = Path(__file__).parent / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write cache
    pretty = config.get("output", {}).get("pretty_print", True)
    with open(output_path, "w") as f:
        json.dump(cache, f, indent=2 if pretty else None)

    print(f"\nCache written to {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"\nSummary:")
    print(f"  Objects: {cache['metadata']['object_count']}")
    print(f"  Columns: {cache['metadata']['column_count']}")
    print(f"  Dependencies: {cache['metadata']['dependency_count']}")
    print(f"  Schemas: {len(cache['indexes']['by_schema'])}")


if __name__ == "__main__":
    main()
