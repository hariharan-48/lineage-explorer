"""
Core domain models for the Exasol Lineage System.
"""
from enum import Enum
from typing import Optional, List, Dict
from pydantic import BaseModel, Field
from datetime import datetime


class ObjectType(str, Enum):
    # Exasol types
    TABLE = "TABLE"
    VIEW = "VIEW"
    LUA_UDF = "LUA_UDF"
    VIRTUAL_SCHEMA = "VIRTUAL_SCHEMA"
    CONNECTION = "CONNECTION"
    # BigQuery types
    BIGQUERY_TABLE = "BIGQUERY_TABLE"
    BIGQUERY_VIEW = "BIGQUERY_VIEW"
    BIGQUERY_UDF = "BIGQUERY_UDF"
    BIGQUERY_PROCEDURE = "BIGQUERY_PROCEDURE"
    # Composer types
    COMPOSER_DAG = "COMPOSER_DAG"


class Platform(str, Enum):
    EXASOL = "exasol"
    BIGQUERY = "bigquery"
    COMPOSER = "composer"


class DatabaseObject(BaseModel):
    """Represents any database object (table, view, UDF, etc.)."""
    id: str  # Format: "SCHEMA.NAME" or "platform:project.dataset.name"
    schema_name: str = Field(alias="schema")
    name: str
    type: ObjectType
    platform: Optional[Platform] = None  # exasol, bigquery, composer
    owner: str
    object_id: int
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    description: Optional[str] = None

    # View-specific fields
    definition: Optional[str] = None

    # UDF-specific fields
    udf_type: Optional[str] = None  # SCALAR or SET
    input_parameters: Optional[List[Dict[str, str]]] = None
    output_columns: Optional[List[Dict[str, str]]] = None
    script_language: Optional[str] = None
    script_text: Optional[str] = None

    # Virtual schema-specific fields
    adapter_name: Optional[str] = None
    connection_name: Optional[str] = None
    remote_schema: Optional[str] = None
    properties: Optional[Dict[str, str]] = None

    # Connection-specific fields
    connection_string: Optional[str] = None
    user: Optional[str] = None

    # Statistics
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None

    class Config:
        populate_by_name = True


class TableLevelDependency(BaseModel):
    """Represents a dependency between two database objects."""
    source_id: str
    target_id: str
    dependency_type: str  # VIEW, ETL, CONNECTION, UDF_INPUT, UDF_OUTPUT, CONSTRAINT
    reference_type: str   # SELECT, INSERT_SELECT, USES, PARAMETER, INSERT
