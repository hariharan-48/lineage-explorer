import { useCallback, useEffect, useRef } from 'react';
import dagre from 'dagre';
import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide,
  type SimulationNodeDatum,
  type SimulationLinkDatum,
} from 'd3-force';
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

interface ForceNode extends SimulationNodeDatum {
  id: string;
  x?: number;
  y?: number;
}

interface ForceLink extends SimulationLinkDatum<ForceNode> {
  source: string | ForceNode;
  target: string | ForceNode;
}

export const useGraphLayout = () => {
  const { nodes, edges, layoutDirection, layoutType, setNodes } = useGraphStore();

  // Track what we've already laid out to prevent infinite loops
  const lastLayoutKey = useRef<string>('');

  // Dagre hierarchical layout (existing)
  const getDagreLayout = useCallback(
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

  // Force-directed layout (new)
  const getForceLayout = useCallback(
    (
      inputNodes: LineageNode[],
      inputEdges: Edge[],
      options: Partial<LayoutOptions> = {}
    ): LineageNode[] => {
      if (inputNodes.length === 0) return inputNodes;

      const opts = { ...DEFAULT_OPTIONS, ...options };

      // Calculate center based on number of nodes
      const centerX = 400;
      const centerY = 300;

      // Create simulation nodes with deterministic initial positions based on node id
      const simNodes: ForceNode[] = inputNodes.map((node, index) => {
        // Use a hash of the node id for deterministic initial placement
        const hash = node.id.split('').reduce((a, b) => ((a << 5) - a + b.charCodeAt(0)) | 0, 0);
        const angle = (hash % 360) * (Math.PI / 180);
        const radius = 100 + (index * 30);
        return {
          id: node.id,
          x: centerX + Math.cos(angle) * radius,
          y: centerY + Math.sin(angle) * radius,
        };
      });

      // Create simulation links
      const simLinks: ForceLink[] = inputEdges
        .filter((edge) => {
          const hasSource = simNodes.some((n) => n.id === edge.source);
          const hasTarget = simNodes.some((n) => n.id === edge.target);
          return hasSource && hasTarget;
        })
        .map((edge) => ({
          source: edge.source,
          target: edge.target,
        }));

      // Create and run force simulation
      const simulation = forceSimulation<ForceNode>(simNodes)
        .force(
          'link',
          forceLink<ForceNode, ForceLink>(simLinks)
            .id((d) => d.id)
            .distance(200) // Distance between connected nodes
            .strength(0.5)
        )
        .force('charge', forceManyBody().strength(-800)) // Repulsion strength
        .force('center', forceCenter(centerX, centerY)) // Center of the layout
        .force(
          'collide',
          forceCollide<ForceNode>()
            .radius(Math.max(opts.nodeWidth, opts.nodeHeight) / 2 + 20)
            .strength(0.8)
        )
        .stop();

      // Run simulation synchronously (300 iterations)
      for (let i = 0; i < 300; i++) {
        simulation.tick();
      }

      // Apply positions to nodes
      const layoutedNodes: LineageNode[] = inputNodes.map((node) => {
        const simNode = simNodes.find((n) => n.id === node.id);
        if (simNode && simNode.x !== undefined && simNode.y !== undefined) {
          return {
            ...node,
            position: {
              x: simNode.x - opts.nodeWidth / 2,
              y: simNode.y - opts.nodeHeight / 2,
            },
          };
        }
        return node;
      });

      return layoutedNodes;
    },
    []
  );

  // Main layout function that chooses based on layoutType
  const getLayoutedElements = useCallback(
    (
      inputNodes: LineageNode[],
      inputEdges: Edge[],
      options: Partial<LayoutOptions> = {}
    ): LineageNode[] => {
      if (layoutType === 'force') {
        return getForceLayout(inputNodes, inputEdges, options);
      }
      return getDagreLayout(inputNodes, inputEdges, options);
    },
    [layoutType, getDagreLayout, getForceLayout]
  );

  // Auto-layout when nodes/edges change or layout type changes
  useEffect(() => {
    if (nodes.length === 0) return;

    // Create a key that represents the current layout state
    const nodeIds = nodes.map(n => n.id).sort().join(',');
    const edgeIds = edges.map(e => e.id).sort().join(',');
    const layoutKey = `${layoutType}-${layoutDirection}-${nodeIds}-${edgeIds}`;

    // Skip if we've already laid out this exact configuration
    if (layoutKey === lastLayoutKey.current) {
      return;
    }

    const layoutedNodes = getLayoutedElements(nodes, edges);
    lastLayoutKey.current = layoutKey;
    setNodes(layoutedNodes);
  }, [nodes, edges, layoutDirection, layoutType, getLayoutedElements, setNodes]);

  return { getLayoutedElements };
};
