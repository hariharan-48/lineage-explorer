import { memo, useCallback } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { LineageNodeData } from '../../store/graphStore';
import { useGraphStore } from '../../store/graphStore';
import './LineageNode.css';

const typeColors: Record<string, string> = {
  TABLE: '#22c55e',
  VIEW: '#3b82f6',
  LUA_UDF: '#f59e0b',
  VIRTUAL_SCHEMA: '#a855f7',
  CONNECTION: '#64748b',
};

const typeIcons: Record<string, string> = {
  TABLE: '📊',
  VIEW: '👁',
  LUA_UDF: '⚙️',
  VIRTUAL_SCHEMA: '🔗',
  CONNECTION: '🔌',
};

export const LineageNode = memo(({ data, id, selected }: NodeProps) => {
  const nodeData = data as LineageNodeData;
  const { object, hasUpstream, hasDownstream, isExpanded, isRoot } = nodeData;
  const { expandNode, setSelectedNode, isLoading } = useGraphStore();

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

  const handleNodeClick = useCallback(() => {
    setSelectedNode(id);
  }, [id, setSelectedNode]);

  const color = typeColors[object.type] || '#64748b';
  const icon = typeIcons[object.type] || '📄';

  const showUpstreamButton = hasUpstream && !isExpanded.upstream;
  const showDownstreamButton = hasDownstream && !isExpanded.downstream;

  return (
    <div
      className={`lineage-node ${selected ? 'selected' : ''} ${isRoot ? 'root' : ''}`}
      onClick={handleNodeClick}
      style={{ '--node-color': color } as React.CSSProperties}
    >
      {/* Upstream expand button */}
      {showUpstreamButton && (
        <button
          className="expand-btn upstream"
          onClick={handleExpandUpstream}
          title="Show upstream dependencies"
          disabled={isLoading}
        >
          +
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
        <span className="type-label">{object.type.replace('_', ' ')}</span>
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

      {/* Downstream expand button */}
      {showDownstreamButton && (
        <button
          className="expand-btn downstream"
          onClick={handleExpandDownstream}
          title="Show downstream dependencies"
          disabled={isLoading}
        >
          +
        </button>
      )}

      {/* Expansion indicators */}
      {isExpanded.upstream && (
        <div className="expanded-indicator upstream" title="Upstream expanded">
          ◂
        </div>
      )}
      {isExpanded.downstream && (
        <div className="expanded-indicator downstream" title="Downstream expanded">
          ▸
        </div>
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
