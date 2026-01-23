"""
API endpoints for lineage traversal.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Literal

from app.models.api import (
    LineageResponse,
    ColumnLineageResponse,
    ObjectColumnLineageResponse,
    ColumnSourceInfo,
    ColumnTargetInfo,
)
from app.services.cache_loader import get_graph_engine
from app.services.graph_engine import LineageGraphEngine

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/{object_id:path}/full", response_model=LineageResponse)
async def get_full_lineage(
    object_id: str,
    upstream_depth: int = Query(default=2, ge=0, le=10),
    downstream_depth: int = Query(default=2, ge=0, le=10),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get full lineage graph for an object (both upstream and downstream).
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")

    result = engine.get_full_lineage(
        object_id,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth,
    )

    return LineageResponse(
        root_object=obj,
        nodes=result.nodes,
        edges=result.edges,
        has_more_upstream=result.has_more_upstream,
        has_more_downstream=result.has_more_downstream,
    )


@router.get("/{object_id:path}/forward", response_model=LineageResponse)
async def get_forward_lineage(
    object_id: str,
    depth: int = Query(default=1, ge=1, le=5),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get downstream (forward) dependencies - objects that depend on this one.
    Use this for incremental expansion with the + button.
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")

    result = engine.get_forward_lineage(object_id, depth)

    return LineageResponse(
        root_object=obj,
        nodes=result.nodes,
        edges=result.edges,
        has_more_upstream=result.has_more_upstream,
        has_more_downstream=result.has_more_downstream,
    )


@router.get("/{object_id:path}/backward", response_model=LineageResponse)
async def get_backward_lineage(
    object_id: str,
    depth: int = Query(default=1, ge=1, le=5),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get upstream (backward) dependencies - objects this one depends on.
    Use this for incremental expansion with the + button.
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")

    result = engine.get_backward_lineage(object_id, depth)

    return LineageResponse(
        root_object=obj,
        nodes=result.nodes,
        edges=result.edges,
        has_more_upstream=result.has_more_upstream,
        has_more_downstream=result.has_more_downstream,
    )


# ========== Column-Level Lineage Endpoints ==========

@router.get("/{object_id:path}/columns", response_model=ObjectColumnLineageResponse)
async def get_object_column_lineage(
    object_id: str,
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get column lineage for all columns of an object.

    Returns column-level dependencies for each column that has lineage data.
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")

    # Get columns with lineage
    columns_with_lineage = engine.get_columns_with_lineage(object_id)
    has_column_lineage = engine.has_column_lineage(object_id)

    # Get lineage for each column
    column_lineage = {}
    for column_name in columns_with_lineage:
        result = engine.get_column_lineage(object_id, column_name, direction="both", depth=1)

        column_lineage[column_name] = ColumnLineageResponse(
            object_id=object_id,
            column_name=column_name,
            dependencies=result.column_deps,
            source_columns=[ColumnSourceInfo(**sc) for sc in result.source_columns],
            target_columns=[ColumnTargetInfo(**tc) for tc in result.target_columns],
        )

    return ObjectColumnLineageResponse(
        object_id=object_id,
        columns_with_lineage=columns_with_lineage,
        column_lineage=column_lineage,
        has_column_lineage=has_column_lineage,
    )


@router.get("/{object_id:path}/columns/{column_name}", response_model=ColumnLineageResponse)
async def get_column_lineage(
    object_id: str,
    column_name: str,
    direction: Literal["upstream", "downstream", "both"] = Query(default="both"),
    depth: int = Query(default=3, ge=1, le=10),
    engine: LineageGraphEngine = Depends(get_graph_engine),
):
    """
    Get full lineage path for a specific column.

    Args:
        object_id: Object ID (e.g., "DWH.MY_VIEW")
        column_name: Column name to trace
        direction: "upstream" (sources), "downstream" (targets), or "both"
        depth: How many levels to traverse (default 3, max 10)

    Returns:
        Column lineage including source columns, target columns, and transformations
    """
    obj = engine.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object not found: {object_id}")

    result = engine.get_column_lineage(object_id, column_name, direction=direction, depth=depth)

    return ColumnLineageResponse(
        object_id=object_id,
        column_name=column_name,
        dependencies=result.column_deps,
        source_columns=[ColumnSourceInfo(**sc) for sc in result.source_columns],
        target_columns=[ColumnTargetInfo(**tc) for tc in result.target_columns],
    )
