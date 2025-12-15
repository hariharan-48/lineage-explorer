"""
Pydantic models for API request/response schemas.
"""
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from .domain import DatabaseObject, TableLevelDependency


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
    schemas: int
    tables: int
    views: int
    udfs: int
    virtual_schemas: int
    connections: int
    cache_loaded_at: Optional[str] = None
