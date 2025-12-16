import { memo, useCallback } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { LineageNodeData } from '../../store/graphStore';
import { useGraphStore } from '../../store/graphStore';
import './LineageNode.css';

const typeColors: Record<string, string> = {
  // Exasol types
  TABLE: '#22c55e',
  VIEW: '#3b82f6',
  LUA_UDF: '#f59e0b',
  VIRTUAL_SCHEMA: '#a855f7',
  CONNECTION: '#64748b',
  // BigQuery types
  BIGQUERY_TABLE: '#4285f4',  // Google Blue
  BIGQUERY_VIEW: '#34a853',   // Google Green
  BIGQUERY_UDF: '#fbbc04',    // Google Yellow
  BIGQUERY_PROCEDURE: '#ea4335', // Google Red
  // Composer types
  COMPOSER_DAG: '#00bfa5',    // Teal (Airflow color)
};

const typeIcons: Record<string, string> = {
  // Exasol types
  TABLE: '📊',
  VIEW: '👁',
  LUA_UDF: '⚙️',
  VIRTUAL_SCHEMA: '🔗',
  CONNECTION: '🔌',
  // BigQuery types
  BIGQUERY_TABLE: '📊',
  BIGQUERY_VIEW: '👁',
  BIGQUERY_UDF: '⚙️',
  BIGQUERY_PROCEDURE: '📜',
  // Composer types
  COMPOSER_DAG: '🔄',
};

// Helper to get display name for type
const getTypeDisplayName = (type: string): string => {
  const displayNames: Record<string, string> = {
    BIGQUERY_TABLE: 'BQ TABLE',
    BIGQUERY_VIEW: 'BQ VIEW',
    BIGQUERY_UDF: 'BQ UDF',
    BIGQUERY_PROCEDURE: 'BQ PROC',
    COMPOSER_DAG: 'DAG',
    LUA_UDF: 'LUA UDF',
  };
  return displayNames[type] || type.replace('_', ' ');
};

export const LineageNode = memo(({ data, id, selected }: NodeProps) => {
  const nodeData = data as LineageNodeData;
  const { object, hasUpstream, hasDownstream, isExpanded, isRoot } = nodeData;
  const {
    expandNode,
    collapseNode,
    setSelectedNode,
    highlightPath,
    clearHighlight,
    highlightedNodeIds,
    isLoading
  } = useGraphStore();

  const isHighlighted = highlightedNodeIds.has(id);
  const hasHighlight = highlightedNodeIds.size > 0;
  const isDimmed = hasHighlight && !isHighlighted;

  const handleExpandUpstream = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (!isLoading) {
        expandNode(id, 'backward');
      }
    },
    [id, expandNode, isLoading]
  );

  const handleExpandDownstream = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (!isLoading) {
        expandNode(id, 'forward');
      }
    },
    [id, expandNode, isLoading]
  );

  const handleCollapseUpstream = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      collapseNode(id, 'backward');
    },
    [id, collapseNode]
  );

  const handleCollapseDownstream = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      collapseNode(id, 'forward');
    },
    [id, collapseNode]
  );

  const handleNodeClick = useCallback(() => {
    setSelectedNode(id);
  }, [id, setSelectedNode]);

  const handleDoubleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      // Toggle highlight - if already highlighting this node's path, clear it
      if (isHighlighted && highlightedNodeIds.size > 0) {
        clearHighlight();
      } else {
        highlightPath(id);
      }
    },
    [id, highlightPath, clearHighlight, isHighlighted, highlightedNodeIds.size]
  );

  const color = typeColors[object.type] || '#64748b';
  const icon = typeIcons[object.type] || '📄';

  // Show + button if has more to expand, show - button if already expanded
  const showUpstreamExpandButton = hasUpstream && !isExpanded.upstream;
  const showUpstreamCollapseButton = isExpanded.upstream && !isRoot;
  const showDownstreamExpandButton = hasDownstream && !isExpanded.downstream;
  const showDownstreamCollapseButton = isExpanded.downstream && !isRoot;

  const nodeClasses = [
    'lineage-node',
    selected ? 'selected' : '',
    isRoot ? 'root' : '',
    isHighlighted ? 'highlighted' : '',
    isDimmed ? 'dimmed' : '',
  ].filter(Boolean).join(' ');

  return (
    <div
      className={nodeClasses}
      onClick={handleNodeClick}
      onDoubleClick={handleDoubleClick}
      style={{ '--node-color': color } as React.CSSProperties}
      title="Double-click to highlight path"
    >
      {/* Upstream expand button (+) */}
      {showUpstreamExpandButton && (
        <button
          className="expand-btn upstream"
          onClick={handleExpandUpstream}
          title="Show upstream dependencies"
          disabled={isLoading}
        >
          +
        </button>
      )}

      {/* Upstream collapse button (-) */}
      {showUpstreamCollapseButton && (
        <button
          className="collapse-btn upstream"
          onClick={handleCollapseUpstream}
          title="Hide upstream dependencies"
        >
          −
        </button>
      )}

      {/* Input handle (left) */}
      <Handle
        type="target"
        position={Position.Left}
        className="handle"
      />

      {/* Node content */}
      <div className="node-header" style={{ backgroundColor: color }}>
        <span className="type-icon">{icon}</span>
        <span className="type-label">{getTypeDisplayName(object.type)}</span>
      </div>

      <div className="node-body">
        <div className="schema-name">{object.schema}</div>
        <div className="object-name">{object.name}</div>
        {object.row_count !== undefined && object.row_count !== null && (
          <div className="row-count">{formatNumber(object.row_count)} rows</div>
        )}
      </div>

      {/* Output handle (right) */}
      <Handle
        type="source"
        position={Position.Right}
        className="handle"
      />

      {/* Downstream expand button (+) */}
      {showDownstreamExpandButton && (
        <button
          className="expand-btn downstream"
          onClick={handleExpandDownstream}
          title="Show downstream dependencies"
          disabled={isLoading}
        >
          +
        </button>
      )}

      {/* Downstream collapse button (-) */}
      {showDownstreamCollapseButton && (
        <button
          className="collapse-btn downstream"
          onClick={handleCollapseDownstream}
          title="Hide downstream dependencies"
        >
          −
        </button>
      )}
    </div>
  );
});

LineageNode.displayName = 'LineageNode';

function formatNumber(num: number): string {
  if (num >= 1_000_000_000) {
    return `${(num / 1_000_000_000).toFixed(1)}B`;
  }
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(1)}K`;
  }
  return num.toString();
}
