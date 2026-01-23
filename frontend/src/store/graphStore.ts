import { create } from 'zustand';
import type { Node, Edge } from '@xyflow/react';
import { api } from '../services/api';
import type { DatabaseObject, LineageResponse, ObjectColumnLineageResponse } from '../types/lineage';

export interface LineageNodeData extends Record<string, unknown> {
  object: DatabaseObject;
  hasUpstream: boolean;
  hasDownstream: boolean;
  isExpanded: {
    upstream: boolean;
    downstream: boolean;
  };
  isRoot: boolean;
}

export type LineageNode = Node<LineageNodeData>;

interface GraphState {
  // Graph data
  nodes: LineageNode[];
  edges: Edge[];
  selectedNodeId: string | null;
  rootObjectId: string | null;

  // Expansion tracking
  expandedNodes: Map<string, { upstream: boolean; downstream: boolean }>;

  // View settings
  layoutDirection: 'TB' | 'LR';
  layoutType: 'dagre' | 'force';
  theme: 'light' | 'dark';

  // Path highlighting
  highlightedNodeIds: Set<string>;
  highlightedEdgeIds: Set<string>;

  // Column lineage state
  showColumnLineage: boolean;
  expandedColumns: Map<string, Set<string>>; // objectId -> Set of expanded column names
  columnLineageCache: Map<string, ObjectColumnLineageResponse>; // objectId -> column lineage data
  selectedColumn: { objectId: string; column: string } | null;

  // Loading states
  isLoading: boolean;
  error: string | null;

  // Actions
  loadLineage: (objectId: string) => Promise<void>;
  expandNode: (nodeId: string, direction: 'forward' | 'backward') => Promise<void>;
  collapseNode: (nodeId: string, direction: 'forward' | 'backward') => void;
  collapseAll: () => void;
  setSelectedNode: (nodeId: string | null) => void;
  setLayoutDirection: (direction: 'TB' | 'LR') => void;
  setLayoutType: (type: 'dagre' | 'force') => void;
  setTheme: (theme: 'light' | 'dark') => void;
  highlightPath: (nodeId: string) => void;
  clearHighlight: () => void;
  setNodes: (nodes: LineageNode[]) => void;
  setEdges: (edges: Edge[]) => void;
  reset: () => void;

  // Column lineage actions
  setShowColumnLineage: (show: boolean) => void;
  toggleColumnExpansion: (objectId: string) => Promise<void>;
  loadColumnLineage: (objectId: string) => Promise<ObjectColumnLineageResponse | null>;
  selectColumn: (objectId: string, column: string) => void;
  clearSelectedColumn: () => void;
}

const transformToReactFlow = (
  lineageData: LineageResponse,
  expandedNodes: Map<string, { upstream: boolean; downstream: boolean }>,
  rootObjectId: string
): { nodes: LineageNode[]; edges: Edge[] } => {
  const nodes: LineageNode[] = Object.values(lineageData.nodes).map((obj) => {
    const expanded = expandedNodes.get(obj.id) || { upstream: false, downstream: false };
    return {
      id: obj.id,
      type: 'lineageNode',
      position: { x: 0, y: 0 },
      data: {
        object: obj,
        hasUpstream: lineageData.has_more_upstream[obj.id] || false,
        hasDownstream: lineageData.has_more_downstream[obj.id] || false,
        isExpanded: expanded,
        isRoot: obj.id === rootObjectId,
      },
    };
  });

  // Color mapping for dependency types
  const dependencyColors: Record<string, string> = {
    'USES': '#3b82f6',           // Blue - view uses table
    'IS DEPENDENT ON': '#8b5cf6', // Purple - general dependency
    'ETL': '#f59e0b',            // Amber - ETL process
    'REFERENCES': '#10b981',      // Green - foreign key reference
    'CALLS': '#ec4899',          // Pink - function/script calls
  };

  const edges: Edge[] = lineageData.edges.map((edge) => {
    const color = dependencyColors[edge.dependency_type] || '#64748b';
    return {
      id: `${edge.source_id}-${edge.target_id}`,
      source: edge.source_id,
      target: edge.target_id,
      type: 'smoothstep',
      animated: true,
      label: edge.dependency_type,
      style: { stroke: color, strokeWidth: 2 },
      labelStyle: { fontSize: 10, fill: color },
      labelBgStyle: { fill: '#f8fafc', fillOpacity: 0.8 },
    };
  });

  return { nodes, edges };
};

export const useGraphStore = create<GraphState>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  rootObjectId: null,
  expandedNodes: new Map(),
  layoutDirection: 'LR',
  layoutType: 'dagre',
  theme: 'light',
  highlightedNodeIds: new Set(),
  highlightedEdgeIds: new Set(),
  // Column lineage state
  showColumnLineage: false,
  expandedColumns: new Map(),
  columnLineageCache: new Map(),
  selectedColumn: null,
  isLoading: false,
  error: null,

  loadLineage: async (objectId: string) => {
    set({ isLoading: true, error: null });
    try {
      // Use depth=1 for initial load to keep the graph manageable
      // Scripts especially can have massive fan-out (reading/writing many tables)
      // Users can expand further using +/- buttons
      const response = await api.getFullLineage(objectId, {
        upstreamDepth: 1,
        downstreamDepth: 1,
      });

      const initialExpanded = new Map<string, { upstream: boolean; downstream: boolean }>();
      initialExpanded.set(objectId, { upstream: true, downstream: true });

      const { nodes, edges } = transformToReactFlow(response, initialExpanded, objectId);

      set({
        nodes,
        edges,
        rootObjectId: objectId,
        expandedNodes: initialExpanded,
        isLoading: false,
        selectedNodeId: objectId,
      });
    } catch (error) {
      set({ error: (error as Error).message, isLoading: false });
    }
  },

  expandNode: async (nodeId: string, direction: 'forward' | 'backward') => {
    const { nodes, edges, expandedNodes, rootObjectId } = get();

    set({ isLoading: true, error: null });
    try {
      const response =
        direction === 'forward'
          ? await api.getForwardLineage(nodeId, { depth: 1 })
          : await api.getBackwardLineage(nodeId, { depth: 1 });

      const existingNodeIds = new Set(nodes.map((n) => n.id));
      const existingEdgeIds = new Set(edges.map((e) => e.id));

      const newExpandedNodes = new Map(expandedNodes);
      Object.keys(response.nodes).forEach((id) => {
        if (!newExpandedNodes.has(id)) {
          newExpandedNodes.set(id, { upstream: false, downstream: false });
        }
      });

      const current = newExpandedNodes.get(nodeId) || { upstream: false, downstream: false };
      newExpandedNodes.set(nodeId, {
        ...current,
        [direction === 'forward' ? 'downstream' : 'upstream']: true,
      });

      const newNodes: LineageNode[] = Object.values(response.nodes)
        .filter((obj) => !existingNodeIds.has(obj.id))
        .map((obj) => ({
          id: obj.id,
          type: 'lineageNode',
          position: { x: 0, y: 0 },
          data: {
            object: obj,
            hasUpstream: response.has_more_upstream[obj.id] || false,
            hasDownstream: response.has_more_downstream[obj.id] || false,
            isExpanded: newExpandedNodes.get(obj.id) || { upstream: false, downstream: false },
            isRoot: obj.id === rootObjectId,
          },
        }));

      // Color mapping for dependency types
      const dependencyColors: Record<string, string> = {
        'USES': '#3b82f6',           // Blue - view uses table
        'IS DEPENDENT ON': '#8b5cf6', // Purple - general dependency
        'ETL': '#f59e0b',            // Amber - ETL process
        'REFERENCES': '#10b981',      // Green - foreign key reference
        'CALLS': '#ec4899',          // Pink - function/script calls
      };

      const newEdges = response.edges
        .filter((e) => !existingEdgeIds.has(`${e.source_id}-${e.target_id}`))
        .map((edge) => {
          const color = dependencyColors[edge.dependency_type] || '#64748b';
          return {
            id: `${edge.source_id}-${edge.target_id}`,
            source: edge.source_id,
            target: edge.target_id,
            type: 'smoothstep',
            animated: true,
            label: edge.dependency_type,
            style: { stroke: color, strokeWidth: 2 },
            labelStyle: { fontSize: 10, fill: color },
            labelBgStyle: { fill: '#f8fafc', fillOpacity: 0.8 },
          };
        });

      const updatedNodes: LineageNode[] = nodes.map((node) => {
        const newExpanded = newExpandedNodes.get(node.id);
        const nodeData = node.data as LineageNodeData;
        if (newExpanded) {
          return {
            ...node,
            data: {
              ...nodeData,
              isExpanded: newExpanded,
              hasUpstream: response.has_more_upstream[node.id] ?? nodeData.hasUpstream,
              hasDownstream: response.has_more_downstream[node.id] ?? nodeData.hasDownstream,
            },
          };
        }
        return node;
      });

      set({
        nodes: [...updatedNodes, ...newNodes],
        edges: [...edges, ...newEdges],
        expandedNodes: newExpandedNodes,
        isLoading: false,
      });
    } catch (error) {
      set({ error: (error as Error).message, isLoading: false });
    }
  },

  collapseNode: (nodeId: string, direction: 'forward' | 'backward') => {
    const { nodes, edges, expandedNodes, rootObjectId } = get();

    // Find all nodes that should be removed (descendants in the given direction)
    const nodesToRemove = new Set<string>();
    const edgesToRemove = new Set<string>();

    // BFS to find all descendant nodes
    const queue = [nodeId];
    const visited = new Set<string>([nodeId]);

    while (queue.length > 0) {
      const currentId = queue.shift()!;

      // Find edges from this node in the specified direction
      edges.forEach((edge) => {
        let childId: string | null = null;

        if (direction === 'forward' && edge.source === currentId) {
          childId = edge.target;
        } else if (direction === 'backward' && edge.target === currentId) {
          childId = edge.source;
        }

        if (childId && !visited.has(childId) && childId !== rootObjectId) {
          visited.add(childId);
          nodesToRemove.add(childId);
          edgesToRemove.add(edge.id);
          queue.push(childId);
        } else if (childId && currentId !== nodeId) {
          // Also remove edges from nodes being removed
          edgesToRemove.add(edge.id);
        }
      });
    }

    // Also remove edges directly connected to nodeId in the collapse direction
    edges.forEach((edge) => {
      if (direction === 'forward' && edge.source === nodeId && nodesToRemove.has(edge.target)) {
        edgesToRemove.add(edge.id);
      } else if (direction === 'backward' && edge.target === nodeId && nodesToRemove.has(edge.source)) {
        edgesToRemove.add(edge.id);
      }
    });

    // Update expanded state
    const newExpandedNodes = new Map(expandedNodes);
    const current = newExpandedNodes.get(nodeId) || { upstream: false, downstream: false };
    newExpandedNodes.set(nodeId, {
      ...current,
      [direction === 'forward' ? 'downstream' : 'upstream']: false,
    });

    // Remove collapsed nodes from expandedNodes
    nodesToRemove.forEach((id) => newExpandedNodes.delete(id));

    // Filter nodes and edges
    const newNodes = nodes
      .filter((node) => !nodesToRemove.has(node.id))
      .map((node) => {
        if (node.id === nodeId) {
          const nodeData = node.data as LineageNodeData;
          return {
            ...node,
            data: {
              ...nodeData,
              isExpanded: newExpandedNodes.get(nodeId) || { upstream: false, downstream: false },
            },
          };
        }
        return node;
      });

    const newEdges = edges.filter((edge) => !edgesToRemove.has(edge.id));

    set({
      nodes: newNodes,
      edges: newEdges,
      expandedNodes: newExpandedNodes,
    });
  },

  collapseAll: () => {
    const { rootObjectId } = get();
    if (rootObjectId) {
      get().loadLineage(rootObjectId);
    }
  },

  setSelectedNode: (nodeId) => set({ selectedNodeId: nodeId }),
  setLayoutDirection: (direction) => set({ layoutDirection: direction }),
  setLayoutType: (type) => set({ layoutType: type }),
  setTheme: (theme) => set({ theme }),

  highlightPath: (nodeId: string) => {
    const { edges } = get();
    const highlightedNodeIds = new Set<string>([nodeId]);
    const highlightedEdgeIds = new Set<string>();

    // Only highlight IMMEDIATE connections (1 level deep)
    // This prevents highlighting the entire graph in densely connected data
    edges.forEach((edge) => {
      // Direct upstream (edge points TO this node)
      if (edge.target === nodeId) {
        highlightedNodeIds.add(edge.source);
        highlightedEdgeIds.add(edge.id);
      }
      // Direct downstream (edge points FROM this node)
      if (edge.source === nodeId) {
        highlightedNodeIds.add(edge.target);
        highlightedEdgeIds.add(edge.id);
      }
    });

    set({ highlightedNodeIds, highlightedEdgeIds });
  },

  clearHighlight: () => {
    set({ highlightedNodeIds: new Set(), highlightedEdgeIds: new Set() });
  },

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),

  // Column lineage actions
  setShowColumnLineage: (show) => set({ showColumnLineage: show }),

  toggleColumnExpansion: async (objectId) => {
    const { expandedColumns, columnLineageCache } = get();
    const newExpandedColumns = new Map(expandedColumns);

    if (newExpandedColumns.has(objectId)) {
      // Collapse - remove from expanded
      newExpandedColumns.delete(objectId);
    } else {
      // Expand - load column lineage if not cached
      let data: ObjectColumnLineageResponse | null | undefined = columnLineageCache.get(objectId);
      if (!data) {
        data = await get().loadColumnLineage(objectId);
      }
      if (data && data.has_column_lineage) {
        newExpandedColumns.set(objectId, new Set(data.columns_with_lineage));
      }
    }

    set({ expandedColumns: newExpandedColumns });
  },

  loadColumnLineage: async (objectId) => {
    const { columnLineageCache } = get();

    // Return from cache if available
    if (columnLineageCache.has(objectId)) {
      return columnLineageCache.get(objectId) ?? null;
    }

    try {
      const data = await api.getObjectColumnLineage(objectId);
      const newCache = new Map(columnLineageCache);
      newCache.set(objectId, data);
      set({ columnLineageCache: newCache });
      return data;
    } catch (error) {
      console.error(`Failed to load column lineage for ${objectId}:`, error);
      return null;
    }
  },

  selectColumn: (objectId, column) => {
    set({ selectedColumn: { objectId, column } });
  },

  clearSelectedColumn: () => {
    set({ selectedColumn: null });
  },

  reset: () =>
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      rootObjectId: null,
      expandedNodes: new Map(),
      highlightedNodeIds: new Set(),
      highlightedEdgeIds: new Set(),
      showColumnLineage: false,
      expandedColumns: new Map(),
      columnLineageCache: new Map(),
      selectedColumn: null,
      error: null,
    }),
}));
