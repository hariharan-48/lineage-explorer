import { useCallback } from 'react';
import { useGraphStore, type LineageNodeData } from '../../store/graphStore';
import './ControlBar.css';

export function ControlBar() {
  const {
    layoutDirection,
    setLayoutDirection,
    layoutType,
    setLayoutType,
    theme,
    setTheme,
    collapseAll,
    nodes,
    edges,
    isLoading,
  } = useGraphStore();

  const exportToExcel = useCallback(() => {
    if (nodes.length === 0) return;

    // Build lineage data for export
    const lineageData: Array<{
      source_schema: string;
      source_name: string;
      source_type: string;
      target_schema: string;
      target_name: string;
      target_type: string;
      dependency_type: string;
    }> = [];

    // Create a map of node IDs to objects
    const nodeMap = new Map(
      nodes.map((n) => [n.id, (n.data as LineageNodeData).object])
    );

    // Build lineage rows from edges
    edges.forEach((edge) => {
      const source = nodeMap.get(edge.source);
      const target = nodeMap.get(edge.target);
      if (source && target) {
        lineageData.push({
          source_schema: source.schema,
          source_name: source.name,
          source_type: source.type,
          target_schema: target.schema,
          target_name: target.name,
          target_type: target.type,
          dependency_type: (edge.label as string) || 'DEPENDENCY',
        });
      }
    });

    // Create CSV content
    const headers = [
      'Source Schema',
      'Source Name',
      'Source Type',
      'Target Schema',
      'Target Name',
      'Target Type',
      'Dependency Type',
    ];
    const csvContent = [
      headers.join(','),
      ...lineageData.map((row) =>
        [
          row.source_schema,
          row.source_name,
          row.source_type,
          row.target_schema,
          row.target_name,
          row.target_type,
          row.dependency_type,
        ].join(',')
      ),
    ].join('\n');

    // Download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `lineage_export_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  }, [nodes, edges]);

  const exportAsImage = useCallback(() => {
    // Find the React Flow viewport and export as PNG
    const viewport = document.querySelector('.react-flow__viewport') as HTMLElement;
    if (!viewport) return;

    // Use html2canvas or similar - for now, we'll use a simpler SVG approach
    const svg = document.querySelector('.react-flow__edges') as SVGElement;
    const nodesContainer = document.querySelector('.react-flow__nodes') as HTMLElement;

    if (!svg || !nodesContainer) {
      alert('Please ensure the graph is visible before exporting');
      return;
    }

    // Create a canvas and draw the viewport
    import('html-to-image').then(({ toPng }) => {
      const flowContainer = document.querySelector('.react-flow') as HTMLElement;
      if (flowContainer) {
        toPng(flowContainer, { backgroundColor: '#f8fafc' })
          .then((dataUrl) => {
            const link = document.createElement('a');
            link.download = `lineage_graph_${new Date().toISOString().split('T')[0]}.png`;
            link.href = dataUrl;
            link.click();
          })
          .catch((err) => {
            console.error('Export failed:', err);
            alert('Image export failed. Please try again.');
          });
      }
    }).catch(() => {
      alert('Image export requires html-to-image package. Export to CSV instead.');
    });
  }, []);

  return (
    <div className="control-bar">
      <div className="control-group">
        <label className="control-label">View</label>
        <div className="button-group">
          <button
            className={`control-btn ${layoutType === 'dagre' ? 'active' : ''}`}
            onClick={() => setLayoutType('dagre')}
            title="Flow View - Hierarchical layout showing lineage direction"
          >
            Flow
          </button>
          <button
            className={`control-btn ${layoutType === 'force' ? 'active' : ''}`}
            onClick={() => setLayoutType('force')}
            title="Graph View - Force-directed layout showing relationships"
          >
            Graph
          </button>
        </div>
      </div>

      <div className="control-group">
        <label className="control-label">Direction</label>
        <div className="button-group">
          <button
            className={`control-btn ${layoutDirection === 'LR' ? 'active' : ''}`}
            onClick={() => setLayoutDirection('LR')}
            title="Left to Right"
            disabled={layoutType === 'force'}
          >
            →
          </button>
          <button
            className={`control-btn ${layoutDirection === 'TB' ? 'active' : ''}`}
            onClick={() => setLayoutDirection('TB')}
            title="Top to Bottom"
            disabled={layoutType === 'force'}
          >
            ↓
          </button>
        </div>
      </div>

      <div className="control-group">
        <button
          className="control-btn reset-btn"
          onClick={collapseAll}
          disabled={isLoading || nodes.length === 0}
          title="Reset to initial view"
        >
          Reset
        </button>
      </div>

      <div className="control-group">
        <label className="control-label">Export</label>
        <div className="button-group">
          <button
            className="control-btn export-btn"
            onClick={exportToExcel}
            disabled={nodes.length === 0}
            title="Export lineage to CSV/Excel"
          >
            📊 CSV
          </button>
          <button
            className="control-btn export-btn"
            onClick={exportAsImage}
            disabled={nodes.length === 0}
            title="Export graph as image"
          >
            🖼️ PNG
          </button>
        </div>
      </div>

      <div className="control-group">
        <label className="control-label">Theme</label>
        <div className="button-group">
          <button
            className={`control-btn ${theme === 'light' ? 'active' : ''}`}
            onClick={() => setTheme('light')}
            title="Light Theme"
          >
            ☀️
          </button>
          <button
            className={`control-btn ${theme === 'dark' ? 'active' : ''}`}
            onClick={() => setTheme('dark')}
            title="Dark Theme"
          >
            🌙
          </button>
        </div>
      </div>

      <div className="control-stats">
        <span className="stat-item">
          <strong>{nodes.length}</strong> nodes
        </span>
        <span className="stat-item">
          <strong>{edges.length}</strong> edges
        </span>
      </div>
    </div>
  );
}
