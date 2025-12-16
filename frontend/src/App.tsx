import { useCallback, useEffect, useMemo } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { useGraphStore, type LineageNodeData, type LineageNode } from './store/graphStore';
import { useGraphLayout } from './hooks/useGraphLayout';
import { LineageNode as LineageNodeComponent } from './components/Nodes/LineageNode';
import { SearchPanel } from './components/Controls/SearchPanel';
import { ControlBar } from './components/Controls/ControlBar';
import { ObjectDetails } from './components/Sidebar/ObjectDetails';
import './App.css';

const nodeTypes = {
  lineageNode: LineageNodeComponent,
};

const miniMapNodeColor = (node: Node): string => {
  const data = node.data as LineageNodeData | undefined;
  const colors: Record<string, string> = {
    TABLE: '#22c55e',
    VIEW: '#3b82f6',
    LUA_UDF: '#f59e0b',
    VIRTUAL_SCHEMA: '#a855f7',
    CONNECTION: '#64748b',
  };
  return colors[data?.object?.type ?? ''] || '#64748b';
};

function App() {
  const {
    nodes: storeNodes,
    edges: storeEdges,
    isLoading,
    error,
    setSelectedNode,
    setNodes: setStoreNodes,
    clearHighlight,
    highlightedEdgeIds,
  } = useGraphStore();

  const [nodes, setNodes, onNodesChange] = useNodesState<LineageNode>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  // Initialize layout hook
  useGraphLayout();

  // Sync store nodes/edges to React Flow state
  useEffect(() => {
    setNodes(storeNodes);
  }, [storeNodes, setNodes]);

  // Apply edge highlighting styles
  const styledEdges = useMemo(() => {
    const hasHighlight = highlightedEdgeIds.size > 0;
    return storeEdges.map((edge) => {
      const isHighlighted = highlightedEdgeIds.has(edge.id);
      const isDimmed = hasHighlight && !isHighlighted;

      return {
        ...edge,
        style: {
          ...edge.style,
          stroke: isHighlighted ? '#f97316' : edge.style?.stroke,
          strokeWidth: isHighlighted ? 4 : isDimmed ? 1 : edge.style?.strokeWidth || 2,
          opacity: isDimmed ? 0.2 : 1,
          transition: 'all 0.3s ease',
        },
        animated: isHighlighted ? true : edge.animated,
        className: isHighlighted ? 'highlighted-edge' : isDimmed ? 'dimmed-edge' : '',
      };
    });
  }, [storeEdges, highlightedEdgeIds]);

  useEffect(() => {
    setEdges(styledEdges);
  }, [styledEdges, setEdges]);

  // Sync React Flow changes back to store (for position updates after drag)
  const handleNodesChange = useCallback(
    (changes: Parameters<typeof onNodesChange>[0]) => {
      onNodesChange(changes);

      // Only sync position changes back to store
      const positionChanges = changes.filter(
        (change) => change.type === 'position' && 'position' in change && change.position
      );
      if (positionChanges.length > 0) {
        setStoreNodes(nodes);
      }
    },
    [onNodesChange, setStoreNodes, nodes]
  );

  const handleNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      setSelectedNode(node.id);
    },
    [setSelectedNode]
  );

  const handlePaneClick = useCallback(() => {
    setSelectedNode(null);
    clearHighlight();
  }, [setSelectedNode, clearHighlight]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-left">
          <h1 className="app-title">Lineage Explorer</h1>
        </div>
        <div className="header-center">
          <SearchPanel />
        </div>
        <div className="header-right">
          {isLoading && <div className="loading-indicator">Loading...</div>}
        </div>
      </header>

      <ControlBar />

      <div className="app-content">
        <div className="graph-container">
          {error && (
            <div className="error-banner">
              <span>Error: {error}</span>
            </div>
          )}

          {nodes.length === 0 && !isLoading && !error && (
            <div className="empty-graph">
              <div className="empty-graph-content">
                <h2>Welcome to Lineage Explorer</h2>
                <p>Search for a database object to visualize its lineage</p>
                <p className="hint">
                  Try searching for: <code>FACT</code>, <code>DIM</code>, <code>VW</code>, or{' '}
                  <code>RPT</code>
                </p>
                <p className="hint">
                  <strong>Tip:</strong> Double-click a node to highlight its connected path
                </p>
              </div>
            </div>
          )}

          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={handleNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={handleNodeClick}
            onPaneClick={handlePaneClick}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.1}
            maxZoom={2}
            defaultEdgeOptions={{
              type: 'smoothstep',
            }}
          >
            <Background color="#e2e8f0" gap={20} />
            <Controls />
            <MiniMap
              nodeColor={miniMapNodeColor}
              nodeStrokeWidth={3}
              zoomable
              pannable
            />
          </ReactFlow>

          {/* Legend */}
          <div className="legend">
            <div className="legend-title">Legend</div>
            <div className="legend-section">
              <div className="legend-section-title">Node Types</div>
              <div className="legend-item">
                <span className="legend-color" style={{ background: '#22c55e' }}></span>
                <span>Table</span>
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{ background: '#3b82f6' }}></span>
                <span>View</span>
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{ background: '#f59e0b' }}></span>
                <span>Lua UDF</span>
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{ background: '#a855f7' }}></span>
                <span>Virtual Schema</span>
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{ background: '#64748b' }}></span>
                <span>Connection</span>
              </div>
            </div>
            <div className="legend-section">
              <div className="legend-section-title">Edge Types</div>
              <div className="legend-item">
                <span className="legend-line" style={{ background: '#3b82f6' }}></span>
                <span>Uses</span>
              </div>
              <div className="legend-item">
                <span className="legend-line" style={{ background: '#8b5cf6' }}></span>
                <span>Is Dependent On</span>
              </div>
              <div className="legend-item">
                <span className="legend-line" style={{ background: '#f59e0b' }}></span>
                <span>ETL</span>
              </div>
              <div className="legend-item">
                <span className="legend-line" style={{ background: '#10b981' }}></span>
                <span>References</span>
              </div>
              <div className="legend-item">
                <span className="legend-line" style={{ background: '#ec4899' }}></span>
                <span>Calls</span>
              </div>
            </div>
            <div className="legend-section">
              <div className="legend-section-title">Interactions</div>
              <div className="legend-hint">
                Double-click to highlight connections
              </div>
              <div className="legend-hint">
                Click empty area to clear
              </div>
            </div>
          </div>
        </div>

        <aside className="sidebar">
          <ObjectDetails />
        </aside>
      </div>
    </div>
  );
}

export default App;
