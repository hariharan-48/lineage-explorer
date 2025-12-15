"""
API endpoints for search and metadata.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, Query

from app.models.domain import DatabaseObject
from app.models.api import SearchResult, StatisticsResponse
from app.services.cache_loader import get_graph_engine, get_cache_loader
from app.services.graph_engine import LineageGraphEngine

router = APIRouter(tags=["search"])


@router.get("/search", response_model=List[SearchResult])
async def search_objects(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    schema: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Search for database objects by name, schema, or ID.
    """
    results = engine.search(
        query=q,
        limit=limit,
        schema_filter=schema,
        type_filter=type,
    )

    return [
        SearchResult(
            id=obj.id,
            schema=obj.schema_name,
            name=obj.name,
            type=obj.type.value,
            description=obj.description,
        )
        for obj in results
    ]


@router.get("/schemas", response_model=List[str])
async def get_schemas(
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get list of all schemas in the database.
    """
    return engine.get_schemas()


@router.get("/types", response_model=List[str])
async def get_types(
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get list of all object types.
    """
    return engine.get_types()


@router.get("/statistics", response_model=StatisticsResponse)
async def get_statistics(
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get cache statistics and metadata.
    """
    stats = engine.get_statistics()
    cache_loader = get_cache_loader()

    return StatisticsResponse(
        total_objects=stats["total_objects"],
        total_dependencies=stats["total_dependencies"],
        schemas=stats["schemas"],
        tables=stats["tables"],
        views=stats["views"],
        udfs=stats["udfs"],
        virtual_schemas=stats["virtual_schemas"],
        connections=stats["connections"],
        cache_loaded_at=cache_loader.loaded_at,
    )
