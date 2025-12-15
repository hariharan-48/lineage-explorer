#!/usr/bin/env python3
"""
Generate realistic sample Exasol lineage data.
Creates deep dependency chains with cross-schema references.
"""
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set
from pathlib import Path


# Configuration
NUM_SCHEMAS = 15
TABLES_PER_SCHEMA = 50
VIEWS_PER_SCHEMA = 30
UDFS_PER_SCHEMA = 10
VIRTUAL_SCHEMAS = 8
CONNECTIONS = 12
MAX_CHAIN_DEPTH = 12
COLUMNS_PER_TABLE = (5, 25)


# Realistic naming patterns
DOMAIN_NAMES = ["SALES", "CUSTOMER", "PRODUCT", "ORDER", "INVENTORY", "FINANCE", "HR", "MARKETING", "SUPPLY", "SHIPPING"]
TABLE_PREFIXES = ["FACT", "DIM", "BRIDGE", "AGG", "STG", "RAW", "TMP", "HIST"]
VIEW_PREFIXES = ["VW", "V", "RPT", "AGG"]
UDF_PREFIXES = ["FN", "PROC", "TRANSFORM", "VALIDATE", "PARSE", "CALC"]

COLUMN_TYPES = {
    "id": ["ID", "KEY", "CODE", "NUM", "SK", "BK"],
    "name": ["NAME", "TITLE", "LABEL", "DESCRIPTION", "FULL_NAME"],
    "date": ["DATE", "TIMESTAMP", "CREATED_AT", "MODIFIED_AT", "EFFECTIVE_DATE", "EXPIRY_DATE"],
    "amount": ["AMOUNT", "TOTAL", "QUANTITY", "COUNT", "VALUE", "PRICE", "COST", "REVENUE"],
    "status": ["STATUS", "STATE", "FLAG", "IS_ACTIVE", "IS_DELETED", "IS_CURRENT"],
    "text": ["COMMENT", "NOTES", "REMARKS", "ADDRESS", "EMAIL", "PHONE", "CITY", "COUNTRY"]
}

DATA_TYPES = {
    "id": "DECIMAL(18,0)",
    "name": "VARCHAR(255)",
    "date": "TIMESTAMP",
    "amount": "DECIMAL(15,2)",
    "status": "VARCHAR(50)",
    "text": "VARCHAR(2000)"
}


class SampleDataGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.objects: Dict[str, dict] = {}
        self.table_deps: List[dict] = []
        self.column_deps: List[dict] = []
        self.object_counter = 100000

    def generate(self) -> dict:
        """Generate complete lineage cache."""
        print("Generating sample data...")

        # Generate in dependency order (sources first)
        self._generate_connections()
        self._generate_virtual_schemas()
        self._generate_raw_tables()
        self._generate_staging_tables()
        self._generate_dwh_tables()
        self._generate_mart_views()
        self._generate_report_views()
        self._generate_udfs()
        self._create_deep_chains()

        print(f"Generated {len(self.objects)} objects")
        print(f"Generated {len(self.table_deps)} table dependencies")
        print(f"Generated {len(self.column_deps)} column dependencies")

        return self._build_cache()

    def _generate_connections(self):
        """Generate external connections."""
        sources = ["ORACLE", "POSTGRES", "MYSQL", "SQLSERVER", "S3", "KAFKA", "SNOWFLAKE", "REDSHIFT"]
        for i in range(CONNECTIONS):
            source = sources[i % len(sources)]
            conn_id = f"SYS.CONN_{source}_{i + 1}"
            self.objects[conn_id] = {
                "id": conn_id,
                "schema": "SYS",
                "name": f"CONN_{source}_{i + 1}",
                "type": "CONNECTION",
                "owner": "SYS",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "description": f"Connection to {source} source system #{i + 1}",
                "connection_string": f"jdbc:{source.lower()}://server{i + 1}:5432/db",
                "user": "ETL_USER"
            }

    def _generate_virtual_schemas(self):
        """Generate virtual schemas tied to connections."""
        connections = [k for k in self.objects.keys() if "CONN_" in k]
        for i in range(VIRTUAL_SCHEMAS):
            conn = connections[i % len(connections)]
            source_name = conn.split("_")[1]
            vs_id = f"EXT.VS_{source_name}_{i + 1}"
            self.objects[vs_id] = {
                "id": vs_id,
                "schema": "EXT",
                "name": f"VS_{source_name}_{i + 1}",
                "type": "VIRTUAL_SCHEMA",
                "owner": "ADMIN_USER",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "description": f"Virtual schema for {source_name} data",
                "adapter_name": f"{source_name}_JDBC",
                "connection_name": conn.split(".")[1],
                "remote_schema": f"{source_name}_SCHEMA"
            }
            # Dependency: Virtual schema uses connection
            self.table_deps.append({
                "source_id": conn,
                "target_id": vs_id,
                "dependency_type": "CONNECTION",
                "reference_type": "USES"
            })

    def _generate_raw_tables(self):
        """Generate raw layer tables from virtual schemas."""
        virtual_schemas = [k for k in self.objects.keys() if k.startswith("EXT.VS_")]

        for domain in DOMAIN_NAMES[:5]:
            schema_name = f"RAW_{domain}"
            for i in range(20):
                table_name = f"RAW_{domain}_{i + 1}"
                table_id = f"{schema_name}.{table_name}"

                columns = self._generate_columns(random.randint(*COLUMNS_PER_TABLE))
                self.objects[table_id] = {
                    "id": table_id,
                    "schema": schema_name,
                    "name": table_name,
                    "type": "TABLE",
                    "owner": "ETL_USER",
                    "object_id": self._next_id(),
                    "created_at": self._random_date().isoformat(),
                    "modified_at": self._random_date().isoformat(),
                    "description": f"Raw {domain} data table #{i + 1}",
                    "columns": columns,
                    "row_count": random.randint(10000, 10000000),
                    "size_bytes": random.randint(1000000, 5000000000)
                }

                # Some tables depend on virtual schemas
                if random.random() < 0.4 and virtual_schemas:
                    vs = random.choice(virtual_schemas)
                    self.table_deps.append({
                        "source_id": vs,
                        "target_id": table_id,
                        "dependency_type": "ETL",
                        "reference_type": "INSERT_SELECT"
                    })

    def _generate_staging_tables(self):
        """Generate staging layer tables."""
        raw_tables = [k for k in self.objects.keys() if ".RAW_" in k]

        for domain in DOMAIN_NAMES[:5]:
            schema_name = f"STG_{domain}"
            for i in range(15):
                table_name = f"STG_{domain}_{i + 1}"
                table_id = f"{schema_name}.{table_name}"

                columns = self._generate_columns(random.randint(*COLUMNS_PER_TABLE))
                self.objects[table_id] = {
                    "id": table_id,
                    "schema": schema_name,
                    "name": table_name,
                    "type": "TABLE",
                    "owner": "ETL_USER",
                    "object_id": self._next_id(),
                    "created_at": self._random_date().isoformat(),
                    "modified_at": self._random_date().isoformat(),
                    "description": f"Staging {domain} table #{i + 1}",
                    "columns": columns,
                    "row_count": random.randint(10000, 5000000),
                    "size_bytes": random.randint(1000000, 2000000000)
                }

                # Dependencies from raw tables
                domain_raw = [t for t in raw_tables if domain in t]
                if domain_raw:
                    source_tables = random.sample(domain_raw, min(3, len(domain_raw)))
                    for src in source_tables:
                        self.table_deps.append({
                            "source_id": src,
                            "target_id": table_id,
                            "dependency_type": "ETL",
                            "reference_type": "INSERT_SELECT"
                        })
                        self._add_column_deps(src, table_id)

    def _generate_dwh_tables(self):
        """Generate DWH fact and dimension tables."""
        staging_tables = [k for k in self.objects.keys() if ".STG_" in k]

        # Fact tables
        for i in range(25):
            domain = random.choice(DOMAIN_NAMES)
            table_name = f"FACT_{domain}_{i + 1}"
            table_id = f"DWH.{table_name}"

            columns = self._generate_columns(random.randint(10, 30), include_measures=True)
            self.objects[table_id] = {
                "id": table_id,
                "schema": "DWH",
                "name": table_name,
                "type": "TABLE",
                "owner": "DWH_USER",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "modified_at": self._random_date().isoformat(),
                "description": f"Fact table for {domain}",
                "columns": columns,
                "row_count": random.randint(1000000, 100000000),
                "size_bytes": random.randint(1000000000, 50000000000)
            }

            # Dependencies from staging
            if staging_tables:
                sources = random.sample(staging_tables, min(5, len(staging_tables)))
                for src in sources:
                    self.table_deps.append({
                        "source_id": src,
                        "target_id": table_id,
                        "dependency_type": "ETL",
                        "reference_type": "INSERT_SELECT"
                    })
                    self._add_column_deps(src, table_id)

        # Dimension tables
        dimensions = ["CUSTOMER", "PRODUCT", "TIME", "GEOGRAPHY", "CHANNEL", "PROMOTION", "EMPLOYEE", "SUPPLIER"]
        for dim in dimensions:
            table_id = f"DWH.DIM_{dim}"
            columns = self._generate_columns(random.randint(8, 20))

            self.objects[table_id] = {
                "id": table_id,
                "schema": "DWH",
                "name": f"DIM_{dim}",
                "type": "TABLE",
                "owner": "DWH_USER",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "modified_at": self._random_date().isoformat(),
                "description": f"{dim} dimension table",
                "columns": columns,
                "row_count": random.randint(1000, 1000000),
                "size_bytes": random.randint(10000000, 500000000)
            }

            # Dependencies from staging
            if staging_tables:
                for src in random.sample(staging_tables, min(2, len(staging_tables))):
                    self.table_deps.append({
                        "source_id": src,
                        "target_id": table_id,
                        "dependency_type": "ETL",
                        "reference_type": "INSERT_SELECT"
                    })

    def _generate_mart_views(self):
        """Generate mart layer views."""
        dwh_tables = [k for k in self.objects.keys() if k.startswith("DWH.")]

        for mart in ["SALES", "FINANCE", "MARKETING", "OPERATIONS"]:
            schema_name = f"MART_{mart}"
            for i in range(20):
                view_name = f"VW_{mart}_{random.choice(DOMAIN_NAMES)}_{i + 1}"
                view_id = f"{schema_name}.{view_name}"

                columns = self._generate_view_columns(random.randint(5, 15))
                self.objects[view_id] = {
                    "id": view_id,
                    "schema": schema_name,
                    "name": view_name,
                    "type": "VIEW",
                    "owner": "ANALYST_USER",
                    "object_id": self._next_id(),
                    "created_at": self._random_date().isoformat(),
                    "modified_at": self._random_date().isoformat(),
                    "description": f"Mart view for {mart} analytics",
                    "definition": f"CREATE VIEW {view_id} AS SELECT ... FROM ...",
                    "columns": columns
                }

                # Dependencies from DWH
                if dwh_tables:
                    sources = random.sample(dwh_tables, min(4, len(dwh_tables)))
                    for src in sources:
                        self.table_deps.append({
                            "source_id": src,
                            "target_id": view_id,
                            "dependency_type": "VIEW",
                            "reference_type": "SELECT"
                        })
                        self._add_column_deps(src, view_id)

    def _generate_report_views(self):
        """Generate report layer views."""
        mart_views = [k for k in self.objects.keys() if k.startswith("MART_")]

        for i in range(40):
            period = random.choice(["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "YTD"])
            domain = random.choice(DOMAIN_NAMES)
            view_name = f"RPT_{period}_{domain}_{i + 1}"
            view_id = f"REPORT.{view_name}"

            columns = self._generate_view_columns(random.randint(3, 10))
            self.objects[view_id] = {
                "id": view_id,
                "schema": "REPORT",
                "name": view_name,
                "type": "VIEW",
                "owner": "REPORT_USER",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "description": f"{period} report for {domain}",
                "definition": f"CREATE VIEW {view_id} AS SELECT ... FROM ...",
                "columns": columns
            }

            # Dependencies from marts
            if mart_views:
                sources = random.sample(mart_views, min(3, len(mart_views)))
                for src in sources:
                    self.table_deps.append({
                        "source_id": src,
                        "target_id": view_id,
                        "dependency_type": "VIEW",
                        "reference_type": "SELECT"
                    })
                    self._add_column_deps(src, view_id)

    def _generate_udfs(self):
        """Generate Lua UDFs."""
        tables = [k for k in self.objects.keys() if self.objects[k]["type"] == "TABLE"]

        for schema_name in ["ETL", "UTILS", "ANALYTICS"]:
            for i in range(15):
                udf_type = random.choice(["SCALAR", "SET"])
                domain = random.choice(DOMAIN_NAMES)
                udf_name = f"{random.choice(UDF_PREFIXES)}_{domain}_{i + 1}"
                udf_id = f"{schema_name}.{udf_name}"

                self.objects[udf_id] = {
                    "id": udf_id,
                    "schema": schema_name,
                    "name": udf_name,
                    "type": "LUA_UDF",
                    "owner": "ETL_USER",
                    "object_id": self._next_id(),
                    "created_at": self._random_date().isoformat(),
                    "description": f"Lua UDF for {domain} processing",
                    "udf_type": udf_type,
                    "input_parameters": [
                        {"name": "input_data", "data_type": "VARCHAR(2000000)"}
                    ],
                    "output_columns": [
                        {"name": "result", "data_type": "VARCHAR(2000000)"},
                        {"name": "status", "data_type": "VARCHAR(50)"}
                    ],
                    "script_language": "LUA",
                    "script_text": f"-- {udf_name} Lua script\nfunction run(ctx)\n  -- Processing logic\nend"
                }

                # Some UDFs process data from tables
                if random.random() < 0.3 and tables:
                    src = random.choice(tables)
                    self.table_deps.append({
                        "source_id": src,
                        "target_id": udf_id,
                        "dependency_type": "UDF_INPUT",
                        "reference_type": "PARAMETER"
                    })

    def _create_deep_chains(self):
        """Create deep dependency chains (12+ levels)."""
        for chain_num in range(5):
            chain_objects = []

            # Level 1: Connection
            conn_id = f"SYS.CONN_CHAIN_{chain_num}"
            self.objects[conn_id] = {
                "id": conn_id,
                "schema": "SYS",
                "name": f"CONN_CHAIN_{chain_num}",
                "type": "CONNECTION",
                "owner": "SYS",
                "object_id": self._next_id(),
                "created_at": self._random_date().isoformat(),
                "connection_string": f"jdbc:chain://server{chain_num}:5432/db",
                "description": f"Deep chain {chain_num} source connection"
            }
            chain_objects.append(conn_id)

            prev_id = conn_id
            for level in range(2, MAX_CHAIN_DEPTH + 1):
                if level == 2:
                    obj_type = "VIRTUAL_SCHEMA"
                    schema = "EXT"
                    name = f"VS_CHAIN_{chain_num}_L{level}"
                    columns = None
                elif level <= 4:
                    obj_type = "TABLE"
                    schema = f"RAW_CHAIN_{chain_num}"
                    name = f"RAW_CHAIN_{chain_num}_L{level}"
                    columns = self._generate_columns(10)
                elif level <= 6:
                    obj_type = "TABLE"
                    schema = f"STG_CHAIN_{chain_num}"
                    name = f"STG_CHAIN_{chain_num}_L{level}"
                    columns = self._generate_columns(12)
                elif level <= 8:
                    obj_type = "TABLE"
                    schema = "DWH"
                    name = f"FACT_CHAIN_{chain_num}_L{level}"
                    columns = self._generate_columns(15, include_measures=True)
                elif level <= 10:
                    obj_type = "VIEW"
                    schema = f"MART_CHAIN_{chain_num}"
                    name = f"VW_CHAIN_{chain_num}_L{level}"
                    columns = self._generate_view_columns(8)
                else:
                    obj_type = "VIEW"
                    schema = "REPORT"
                    name = f"RPT_CHAIN_{chain_num}_L{level}"
                    columns = self._generate_view_columns(6)

                obj_id = f"{schema}.{name}"

                obj_data = {
                    "id": obj_id,
                    "schema": schema,
                    "name": name,
                    "type": obj_type,
                    "owner": "CHAIN_USER",
                    "object_id": self._next_id(),
                    "created_at": self._random_date().isoformat(),
                    "description": f"Deep chain {chain_num} level {level}"
                }

                if columns:
                    obj_data["columns"] = columns

                if obj_type == "VIEW":
                    obj_data["definition"] = f"CREATE VIEW {obj_id} AS SELECT ... FROM {prev_id}"

                if obj_type == "VIRTUAL_SCHEMA":
                    obj_data["adapter_name"] = "CHAIN_JDBC"
                    obj_data["connection_name"] = prev_id.split(".")[1]

                self.objects[obj_id] = obj_data

                # Add dependency from previous level
                dep_type = "CONNECTION" if level == 2 else ("VIEW" if obj_type == "VIEW" else "ETL")
                self.table_deps.append({
                    "source_id": prev_id,
                    "target_id": obj_id,
                    "dependency_type": dep_type,
                    "reference_type": "USES" if dep_type == "CONNECTION" else "SELECT"
                })

                if columns and prev_id in self.objects and self.objects[prev_id].get("columns"):
                    self._add_column_deps(prev_id, obj_id)

                chain_objects.append(obj_id)
                prev_id = obj_id

    def _generate_columns(self, count: int, include_measures: bool = False) -> List[dict]:
        """Generate realistic columns for a table."""
        columns = []

        # Always add ID column
        columns.append({
            "name": "ID",
            "data_type": "DECIMAL(18,0)",
            "ordinal_position": 1,
            "is_nullable": False,
            "is_primary_key": True,
            "description": "Primary key"
        })

        col_types = list(COLUMN_TYPES.keys())
        used_names: Set[str] = {"ID"}

        for i in range(2, count + 1):
            col_type = random.choice(col_types)
            col_name_base = random.choice(COLUMN_TYPES[col_type])

            # Ensure unique column names
            col_name = col_name_base
            suffix = 1
            while col_name in used_names:
                col_name = f"{col_name_base}_{suffix}"
                suffix += 1
            used_names.add(col_name)

            columns.append({
                "name": col_name,
                "data_type": DATA_TYPES[col_type],
                "ordinal_position": i,
                "is_nullable": random.random() > 0.3,
                "is_primary_key": False,
                "description": f"{col_type.title()} field"
            })

        return columns

    def _generate_view_columns(self, count: int) -> List[dict]:
        """Generate columns for a view with source tracking."""
        columns = self._generate_columns(count)
        for col in columns:
            col["source_columns"] = []
        return columns

    def _add_column_deps(self, source_id: str, target_id: str):
        """Add column-level dependencies between two objects."""
        source_obj = self.objects.get(source_id)
        target_obj = self.objects.get(target_id)

        if not source_obj or not target_obj:
            return

        source_cols = source_obj.get("columns", [])
        target_cols = target_obj.get("columns", [])

        if not source_cols or not target_cols:
            return

        # Create some column mappings
        num_mappings = min(len(source_cols), len(target_cols), random.randint(2, 5))
        transformations = [None, "CAST", "COALESCE", "TRIM", "UPPER", "SUM", "COUNT", "AVG", "MAX", "MIN"]

        for _ in range(num_mappings):
            src_col = random.choice(source_cols)
            tgt_col = random.choice(target_cols)

            self.column_deps.append({
                "source_object_id": source_id,
                "source_column": src_col["name"],
                "target_object_id": target_id,
                "target_column": tgt_col["name"],
                "transformation": random.choice(transformations)
            })

    def _next_id(self) -> int:
        self.object_counter += 1
        return self.object_counter

    def _random_date(self) -> datetime:
        days_ago = random.randint(0, 365 * 2)
        return datetime.now() - timedelta(days=days_ago)

    def _build_cache(self) -> dict:
        """Build final cache structure with indexes."""
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
            forward_edges[src].append(tgt)

            if tgt not in backward_edges:
                backward_edges[tgt] = []
            backward_edges[tgt].append(src)

        return {
            "metadata": {
                "version": "1.0.0",
                "generated_at": datetime.now().isoformat(),
                "source_database": "EXASOL_SAMPLE",
                "object_count": len(self.objects),
                "column_count": sum(len(obj.get("columns", [])) for obj in self.objects.values()),
                "dependency_count": len(self.table_deps)
            },
            "objects": self.objects,
            "dependencies": {
                "table_level": self.table_deps,
                "column_level": self.column_deps
            },
            "indexes": {
                "by_schema": by_schema,
                "by_type": by_type,
                "forward_edges": forward_edges,
                "backward_edges": backward_edges
            }
        }


def main():
    generator = SampleDataGenerator(seed=42)
    cache = generator.generate()

    output_path = Path(__file__).parent.parent / "data" / "lineage_cache.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\nCache written to {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

    # Print summary
    print(f"\nSummary:")
    print(f"  Objects: {cache['metadata']['object_count']}")
    print(f"  Columns: {cache['metadata']['column_count']}")
    print(f"  Dependencies: {cache['metadata']['dependency_count']}")
    print(f"  Schemas: {len(cache['indexes']['by_schema'])}")


if __name__ == "__main__":
    main()
