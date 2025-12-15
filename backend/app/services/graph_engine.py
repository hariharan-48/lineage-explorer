"""
Core graph engine for efficient lineage traversal.
Uses pre-built adjacency lists for O(1) neighbor lookups.
"""
from typing import Dict, Set, List, Optional, Any
from collections import deque
from dataclasses import dataclass

from app.models.domain import (
    DatabaseObject,
    TableLevelDependency,
)


@dataclass
class LineageResult:
    """Result of a lineage traversal."""
    nodes: Dict[str, DatabaseObject]
    edges: List[TableLevelDependency]
    has_more_upstream: Dict[str, bool]
    has_more_downstream: Dict[str, bool]


class LineageGraphEngine:
    """
    High-performance graph engine for lineage traversal.

    Key optimizations:
    1. Pre-built adjacency lists for O(1) neighbor lookup
    2. Bidirectional edges stored separately for forward/backward traversal
    3. Visited set tracking to handle cycles
    4. Lazy loading support for incremental expansion
    """

    def __init__(self):
        self._objects: Dict[str, DatabaseObject] = {}
        self._forward_edges: Dict[str, Set[str]] = {}  # source -> targets (downstream)
        self._backward_edges: Dict[str, Set[str]] = {}  # target -> sources (upstream)
        self._table_deps: List[TableLevelDependency] = []

        # Indexes for fast filtering
        self._by_schema: Dict[str, Set[str]] = {}
        self._by_type: Dict[str, Set[str]] = {}

        # Edge lookup map
        self._edge_map: Dict[tuple, TableLevelDependency] = {}

    def load_cache(self, cache_data: dict) -> None:
        """
        Load and index the JSON cache data.
        Time complexity: O(n + m) where n = objects, m = dependencies
        """
        # Load objects
        for obj_id, obj_data in cache_data.get("objects", {}).items():
            # Handle schema field alias
            if "schema" in obj_data:
                obj_data["schema_name"] = obj_data.pop("schema")
            self._objects[obj_id] = DatabaseObject(**obj_data)

            # Build indexes
            schema = obj_data.get("schema_name", obj_data.get("schema", ""))
            obj_type = obj_data["type"]

            if schema not in self._by_schema:
                self._by_schema[schema] = set()
            self._by_schema[schema].add(obj_id)

            if obj_type not in self._by_type:
                self._by_type[obj_type] = set()
            self._by_type[obj_type].add(obj_id)

        # Build adjacency lists from dependencies
        deps = cache_data.get("dependencies", {})

        for dep_data in deps.get("table_level", []):
            dep = TableLevelDependency(**dep_data)
            self._table_deps.append(dep)

            source, target = dep.source_id, dep.target_id

            # Forward: source -> target (what does source feed into?)
            if source not in self._forward_edges:
                self._forward_edges[source] = set()
            self._forward_edges[source].add(target)

            # Backward: target -> source (what does target depend on?)
            if target not in self._backward_edges:
                self._backward_edges[target] = set()
            self._backward_edges[target].add(source)

            # Edge lookup
            self._edge_map[(source, target)] = dep

    def get_object(self, object_id: str) -> Optional[DatabaseObject]:
        """Get a single object by ID."""
        return self._objects.get(object_id)

    def get_all_objects(self) -> Dict[str, DatabaseObject]:
        """Get all objects."""
        return self._objects

    def get_forward_lineage(
        self,
        object_id: str,
        depth: int = 1,
        visited: Optional[Set[str]] = None,
    ) -> LineageResult:
        """
        Get downstream dependencies (objects that depend on this one).
        Uses BFS for level-controlled expansion.

        Args:
            object_id: Starting object ID
            depth: How many levels to traverse (default 1)
            visited: Already visited nodes (for incremental expansion)
        """
        return self._traverse(
            object_id,
            depth,
            direction="forward",
            visited=visited,
        )

    def get_backward_lineage(
        self,
        object_id: str,
        depth: int = 1,
        visited: Optional[Set[str]] = None,
    ) -> LineageResult:
        """
        Get upstream dependencies (objects this one depends on).
        Uses BFS for level-controlled expansion.
        """
        return self._traverse(
            object_id,
            depth,
            direction="backward",
            visited=visited,
        )

    def get_full_lineage(
        self,
        object_id: str,
        upstream_depth: int = 3,
        downstream_depth: int = 3,
    ) -> LineageResult:
        """
        Get both upstream and downstream lineage from a starting point.
        """
        # Get upstream (backward) lineage
        upstream = self.get_backward_lineage(object_id, upstream_depth)

        # Get downstream (forward) lineage
        downstream = self.get_forward_lineage(object_id, downstream_depth)

        # Merge results
        all_nodes = {**upstream.nodes, **downstream.nodes}
        if object_id in self._objects:
            all_nodes[object_id] = self._objects[object_id]

        # Merge and deduplicate edges
        all_edges = upstream.edges + downstream.edges
        seen_edges = set()
        unique_edges = []
        for edge in all_edges:
            key = (edge.source_id, edge.target_id)
            if key not in seen_edges:
                seen_edges.add(key)
                unique_edges.append(edge)

        # Merge has_more flags
        has_more_upstream = {**upstream.has_more_upstream, **downstream.has_more_upstream}
        has_more_downstream = {**upstream.has_more_downstream, **downstream.has_more_downstream}

        return LineageResult(
            nodes=all_nodes,
            edges=unique_edges,
            has_more_upstream=has_more_upstream,
            has_more_downstream=has_more_downstream,
        )

    def _traverse(
        self,
        start_id: str,
        depth: int,
        direction: str,
        visited: Optional[Set[str]] = None,
    ) -> LineageResult:
        """
        BFS traversal in specified direction.

        Time complexity: O(V + E) for the subgraph visited
        Space complexity: O(V) for visited set and result storage
        """
        if visited is None:
            visited = set()

        edges_map = self._forward_edges if direction == "forward" else self._backward_edges

        result_nodes: Dict[str, DatabaseObject] = {}
        result_edges: List[TableLevelDependency] = []
        has_more_upstream: Dict[str, bool] = {}
        has_more_downstream: Dict[str, bool] = {}

        # BFS with depth tracking
        queue: deque = deque([(start_id, 0)])
        visited.add(start_id)

        while queue:
            current_id, current_depth = queue.popleft()

            if current_id not in self._objects:
                continue

            result_nodes[current_id] = self._objects[current_id]

            # Check for unexpanded edges
            forward_neighbors = self._forward_edges.get(current_id, set())
            backward_neighbors = self._backward_edges.get(current_id, set())

            # Calculate has_more based on unexpanded neighbors
            has_more_downstream[current_id] = any(n not in visited for n in forward_neighbors)
            has_more_upstream[current_id] = any(n not in visited for n in backward_neighbors)

            # Only expand if within depth limit
            if current_depth < depth:
                neighbors = edges_map.get(current_id, set())
                for neighbor_id in neighbors:
                    # Add edge
                    if direction == "forward":
                        edge = self._edge_map.get((current_id, neighbor_id))
                    else:
                        edge = self._edge_map.get((neighbor_id, current_id))

                    if edge:
                        result_edges.append(edge)

                    # Add to queue if not visited (handles cycles)
                    if neighbor_id not in visited:
                        visited.add(neighbor_id)
                        queue.append((neighbor_id, current_depth + 1))

        return LineageResult(
            nodes=result_nodes,
            edges=result_edges,
            has_more_upstream=has_more_upstream,
            has_more_downstream=has_more_downstream,
        )

    def search(
        self,
        query: str,
        limit: int = 50,
        schema_filter: Optional[str] = None,
        type_filter: Optional[str] = None,
    ) -> List[DatabaseObject]:
        """
        Search objects by name with optional filters.
        Uses case-insensitive substring matching.
        """
        query_lower = query.lower()
        results = []

        # Determine candidate set based on filters
        if schema_filter and type_filter:
            candidates = self._by_schema.get(schema_filter, set()) & self._by_type.get(
                type_filter, set()
            )
        elif schema_filter:
            candidates = self._by_schema.get(schema_filter, set())
        elif type_filter:
            candidates = self._by_type.get(type_filter, set())
        else:
            candidates = set(self._objects.keys())

        for obj_id in candidates:
            obj = self._objects[obj_id]
            # Match against name, schema, or full ID
            if (
                query_lower in obj.name.lower()
                or query_lower in obj.schema_name.lower()
                or query_lower in obj_id.lower()
            ):
                results.append(obj)
                if len(results) >= limit:
                    break

        return results

    def get_schemas(self) -> List[str]:
        """Get list of all schemas."""
        return sorted(self._by_schema.keys())

    def get_types(self) -> List[str]:
        """Get list of all object types."""
        return sorted(self._by_type.keys())

    def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "total_objects": len(self._objects),
            "total_dependencies": len(self._table_deps),
            "schemas": len(self._by_schema),
            "tables": len(self._by_type.get("TABLE", set())),
            "views": len(self._by_type.get("VIEW", set())),
            "udfs": len(self._by_type.get("LUA_UDF", set())),
            "virtual_schemas": len(self._by_type.get("VIRTUAL_SCHEMA", set())),
            "connections": len(self._by_type.get("CONNECTION", set())),
        }

    def get_objects_paginated(
        self,
        page: int = 1,
        page_size: int = 50,
        schema_filter: Optional[str] = None,
        type_filter: Optional[str] = None,
    ) -> tuple:
        """Get paginated list of objects with optional filters."""
        # Determine candidates
        if schema_filter and type_filter:
            candidates = list(
                self._by_schema.get(schema_filter, set())
                & self._by_type.get(type_filter, set())
            )
        elif schema_filter:
            candidates = list(self._by_schema.get(schema_filter, set()))
        elif type_filter:
            candidates = list(self._by_type.get(type_filter, set()))
        else:
            candidates = list(self._objects.keys())

        # Sort for consistent pagination
        candidates.sort()

        total = len(candidates)
        start = (page - 1) * page_size
        end = start + page_size

        items = [self._objects[oid] for oid in candidates[start:end]]

        return items, total
