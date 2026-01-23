import { useGraphStore } from '../../store/graphStore';
import type { DatabaseObject, ColumnLineageResponse } from '../../types/lineage';
import './ObjectDetails.css';

const typeColors: Record<string, string> = {
  TABLE: '#22c55e',
  VIEW: '#3b82f6',
  LUA_UDF: '#f59e0b',
  VIRTUAL_SCHEMA: '#a855f7',
  CONNECTION: '#64748b',
};

export function ObjectDetails() {
  const { nodes, selectedNodeId, selectedColumn, columnLineageCache, showColumnLineage } = useGraphStore();

  const selectedNode = nodes.find((n) => n.id === selectedNodeId);
  const object = selectedNode?.data?.object as DatabaseObject | undefined;

  // Get column lineage data for selected column
  let columnLineage: ColumnLineageResponse | null = null;
  if (selectedColumn && columnLineageCache.has(selectedColumn.objectId)) {
    const objLineage = columnLineageCache.get(selectedColumn.objectId);
    if (objLineage) {
      // Try exact match first
      if (objLineage.column_lineage[selectedColumn.column]) {
        columnLineage = objLineage.column_lineage[selectedColumn.column];
      } else {
        // Try case-insensitive match
        const colUpper = selectedColumn.column.toUpperCase();
        const matchingKey = Object.keys(objLineage.column_lineage).find(
          key => key.toUpperCase() === colUpper
        );
        if (matchingKey) {
          columnLineage = objLineage.column_lineage[matchingKey];
        }
      }
    }
  }

  if (!object) {
    return (
      <div className="object-details empty">
        <div className="empty-state">
          <div className="empty-icon">📊</div>
          <div className="empty-title">No object selected</div>
          <div className="empty-text">
            Search for an object or click on a node to view details
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="object-details">
      <div className="object-header">
        <div
          className="object-type-badge"
          style={{ backgroundColor: typeColors[object.type] || '#64748b' }}
        >
          {object.type.replace('_', ' ')}
        </div>
        <h2 className="object-title">{object.name}</h2>
        <div className="object-schema">{object.schema}</div>
      </div>

      {object.description && (
        <div className="detail-section">
          <div className="section-content description">{object.description}</div>
        </div>
      )}

      <div className="detail-section">
        <h3 className="section-title">Properties</h3>
        <div className="properties-grid">
          <div className="property">
            <span className="property-label">Owner</span>
            <span className="property-value">{object.owner}</span>
          </div>
          <div className="property">
            <span className="property-label">Created</span>
            <span className="property-value">
              {new Date(object.created_at).toLocaleDateString()}
            </span>
          </div>
          {object.modified_at && (
            <div className="property">
              <span className="property-label">Modified</span>
              <span className="property-value">
                {new Date(object.modified_at).toLocaleDateString()}
              </span>
            </div>
          )}
          {object.row_count !== undefined && object.row_count !== null && (
            <div className="property">
              <span className="property-label">Rows</span>
              <span className="property-value">{formatNumber(object.row_count)}</span>
            </div>
          )}
          {object.size_bytes !== undefined && object.size_bytes !== null && (
            <div className="property">
              <span className="property-label">Size</span>
              <span className="property-value">{formatBytes(object.size_bytes)}</span>
            </div>
          )}
          {object.udf_type && (
            <div className="property">
              <span className="property-label">UDF Type</span>
              <span className="property-value">{object.udf_type}</span>
            </div>
          )}
          {object.adapter_name && (
            <div className="property">
              <span className="property-label">Adapter</span>
              <span className="property-value">{object.adapter_name}</span>
            </div>
          )}
          {object.connection_name && (
            <div className="property">
              <span className="property-label">Connection</span>
              <span className="property-value">{object.connection_name}</span>
            </div>
          )}
        </div>
      </div>

      {object.definition && (
        <div className="detail-section">
          <h3 className="section-title">Definition</h3>
          <pre className="definition-code">{object.definition}</pre>
        </div>
      )}

      {/* Column lineage section */}
      {showColumnLineage && selectedColumn && columnLineage && (
        <div className="detail-section">
          <h3 className="section-title">Column Lineage</h3>
          <div className="column-lineage-info">
            <div className="column-lineage-header">
              <span className="column-lineage-name">{selectedColumn.column}</span>
            </div>

            {columnLineage.source_columns.length > 0 && (
              <div className="column-lineage-group">
                <div className="group-label">Source Columns:</div>
                {columnLineage.source_columns.map((src, idx) => (
                  <div key={idx} className="column-lineage-item source">
                    <span className="source-object">{src.object_id}</span>
                    <span className="source-column">.{src.column}</span>
                    {src.transformation && (
                      <span className="transformation">{src.transformation}</span>
                    )}
                    {src.transformation_type !== 'DIRECT' && (
                      <span className={`transform-type ${src.transformation_type.toLowerCase()}`}>
                        {src.transformation_type}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            )}

            {columnLineage.target_columns.length > 0 && (
              <div className="column-lineage-group">
                <div className="group-label">Target Columns:</div>
                {columnLineage.target_columns.map((tgt, idx) => (
                  <div key={idx} className="column-lineage-item target">
                    <span className="target-object">{tgt.object_id}</span>
                    <span className="target-column">.{tgt.column}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function formatNumber(num: number): string {
  return new Intl.NumberFormat().format(num);
}

function formatBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) {
    return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
  }
  if (bytes >= 1_000_000) {
    return `${(bytes / 1_000_000).toFixed(2)} MB`;
  }
  if (bytes >= 1_000) {
    return `${(bytes / 1_000).toFixed(2)} KB`;
  }
  return `${bytes} B`;
}
