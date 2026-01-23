"""
Pydantic models for API request/response schemas.
"""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from .domain import DatabaseObject, TableLevelDependency, ColumnLevelDependency


class LineageResponse(BaseModel):
    """Response for lineage queries."""
    root_object: DatabaseObject
    nodes: Dict[str, DatabaseObject]
    edges: List[TableLevelDependency]
    has_more_upstream: Dict[str, bool]
    has_more_downstream: Dict[str, bool]


class ObjectListResponse(BaseModel):
    """Paginated list of objects."""
    items: List[DatabaseObject]
    total: int
    page: int
    page_size: int
    total_pages: int


class SearchResult(BaseModel):
    """Single search result."""
    id: str
    schema_name: str = Field(alias="schema")
    name: str
    type: str
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class StatisticsResponse(BaseModel):
    """Cache statistics."""
    total_objects: int
    total_dependencies: int
    total_column_dependencies: int = 0
    objects_with_column_lineage: int = 0
    schemas: int
    tables: int
    views: int
    udfs: int
    virtual_schemas: int
    connections: int
    cache_loaded_at: Optional[str] = None


class ColumnSourceInfo(BaseModel):
    """Information about a source column in column lineage."""
    object_id: str
    column: str
    transformation: Optional[str] = None
    transformation_type: str = "DIRECT"


class ColumnTargetInfo(BaseModel):
    """Information about a target column in column lineage."""
    object_id: str
    column: str


class ColumnLineageResponse(BaseModel):
    """Response for column lineage query."""
    object_id: str
    column_name: str
    dependencies: List[ColumnLevelDependency]
    source_columns: List[ColumnSourceInfo]
    target_columns: List[ColumnTargetInfo]


class ObjectColumnLineageResponse(BaseModel):
    """Response for all column lineage of an object."""
    object_id: str
    columns_with_lineage: List[str]
    column_lineage: Dict[str, ColumnLineageResponse]
    has_column_lineage: bool
