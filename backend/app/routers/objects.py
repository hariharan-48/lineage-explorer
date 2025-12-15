"""
API endpoints for database objects.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query

from app.models.domain import DatabaseObject
from app.models.api import ObjectListResponse
from app.services.cache_loader import get_graph_engine
from app.services.graph_engine import LineageGraphEngine

router = APIRouter(prefix="/objects", tags=["objects"])


@router.get("", response_model=ObjectListResponse)
async def list_objects(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    schema: Optional[str] = Query(default=None, alias="schema"),
    type: Optional[str] = Query(default=None),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    List all database objects with pagination and optional filters.
    """
    items, total = engine.get_objects_paginated(
        page=page,
        page_size=page_size,
        schema_filter=schema,
        type_filter=type,
    )

    total_pages = (total + page_size - 1) // page_size

    return ObjectListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/{object_id:path}", response_model=DatabaseObject)
async def get_object(
    object_id: str,
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get details for a specific database object.
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")
    return obj
