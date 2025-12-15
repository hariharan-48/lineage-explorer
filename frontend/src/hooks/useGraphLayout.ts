import { useCallback, useEffect } from 'react';
import dagre from 'dagre';
import type { Edge } from '@xyflow/react';
import { useGraphStore, type LineageNode } from '../store/graphStore';

interface LayoutOptions {
  direction: 'TB' | 'LR';
  nodeWidth: number;
  nodeHeight: number;
  rankSep: number;
  nodeSep: number;
}

const DEFAULT_OPTIONS: LayoutOptions = {
  direction: 'LR',
  nodeWidth: 250,
  nodeHeight: 120,
  rankSep: 80,
  nodeSep: 40,
};

export const useGraphLayout = () => {
  const { nodes, edges, layoutDirection, setNodes } = useGraphStore();

  const getLayoutedElements = useCallback(
    (
      inputNodes: LineageNode[],
      inputEdges: Edge[],
      options: Partial<LayoutOptions> = {}
    ): LineageNode[] => {
      if (inputNodes.length === 0) return inputNodes;

      const opts = { ...DEFAULT_OPTIONS, ...options, direction: layoutDirection };

      const dagreGraph = new dagre.graphlib.Graph();
      dagreGraph.setDefaultEdgeLabel(() => ({}));
      dagreGraph.setGraph({
        rankdir: opts.direction,
        ranksep: opts.rankSep,
        nodesep: opts.nodeSep,
        marginx: 50,
        marginy: 50,
      });

      // Add nodes to dagre
      inputNodes.forEach((node) => {
        dagreGraph.setNode(node.id, {
          width: opts.nodeWidth,
          height: opts.nodeHeight,
        });
      });

      // Add edges to dagre
      inputEdges.forEach((edge) => {
        if (dagreGraph.hasNode(edge.source) && dagreGraph.hasNode(edge.target)) {
          dagreGraph.setEdge(edge.source, edge.target);
        }
      });

      // Calculate layout
      dagre.layout(dagreGraph);

      // Apply positions to nodes
      const layoutedNodes: LineageNode[] = inputNodes.map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        if (nodeWithPosition) {
          return {
            ...node,
            position: {
              x: nodeWithPosition.x - opts.nodeWidth / 2,
              y: nodeWithPosition.y - opts.nodeHeight / 2,
            },
          };
        }
        return node;
      });

      return layoutedNodes;
    },
    [layoutDirection]
  );

  // Auto-layout when nodes/edges change
  useEffect(() => {
    if (nodes.length > 0) {
      const layoutedNodes = getLayoutedElements(nodes, edges);

      // Only update if positions changed
      const positionsChanged = layoutedNodes.some((node, index) => {
        const originalNode = nodes[index];
        return (
          originalNode &&
          (node.position.x !== originalNode.position.x ||
            node.position.y !== originalNode.position.y)
        );
      });

      if (positionsChanged) {
        setNodes(layoutedNodes);
      }
    }
  }, [nodes.length, edges.length, layoutDirection, getLayoutedElements, setNodes, nodes, edges]);

  return { getLayoutedElements };
};
