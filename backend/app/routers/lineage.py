"""
API endpoints for lineage traversal.
"""
from fastapi import APIRouter, Depends, HTTPException, Query

from app.models.api import LineageResponse
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
