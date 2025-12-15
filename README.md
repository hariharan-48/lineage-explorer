# Exasol Lineage Explorer

A beautiful, interactive data lineage visualization tool for Exasol databases. Explore your database objects and their dependencies with an intuitive graph interface.

## Features

- **Interactive Graph Visualization**: Explore table-level lineage with React Flow
- **Forward/Backward Navigation**: Click +/- buttons to expand lineage in either direction
- **Multiple Object Types**: Tables, Views, Lua UDFs, Virtual Schemas, Connections
- **Search**: Find any object instantly with autocomplete
- **Details Panel**: View object metadata and definitions
- **Export Options**: Export lineage to CSV (for Excel) or PNG image
- **Configurable Layout**: Switch between horizontal (LR) and vertical (TB) layouts
- **Plug & Play**: Configure for any Exasol environment

## Quick Start

### Option 1: Using Sample Data (Demo)

```bash
# Clone the repository
git clone <repository-url>
cd lineage

# Create virtual environment
python3 -m venv .lineage
source .lineage/bin/activate

# Install backend dependencies
pip install -r backend/requirements.txt

# Generate sample data
cd backend
python scripts/generate_sample_data.py

# Start the backend
uvicorn app.main:app --reload --port 8000
```

In another terminal:

```bash
# Install frontend dependencies
cd frontend
npm install

# Start the frontend
npm run dev
```

Open http://localhost:5173 in your browser.

### Option 2: Connect to Your Exasol Database

1. Install dependencies (already in requirements.txt):
```bash
pip install pyexasol pyyaml sqlglot luaparser
```

2. Configure your connection:
```bash
cd backend/scripts
cp exasol_config.yaml exasol_config.local.yaml
# Edit exasol_config.local.yaml with your connection details
```

3. Extract lineage from your database:
```bash
python extract_from_exasol.py
```

4. Start the application (same as above)

### Option 3: Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up --build

# Access at http://localhost:3000
```

## Configuration

### Exasol Connection (`backend/scripts/exasol_config.yaml`)

```yaml
connection:
  host: "your-exasol-host.com"
  port: 8563
  user: "your_username"
  password: "your_password"

extraction:
  # Filter schemas to include
  include_schemas:
    - "DWH"
    - "STAGING"
    - "MART"

  # Schemas to always exclude
  exclude_schemas:
    - "SYS"
    - "EXA_STATISTICS"

  # What to extract
  object_types:
    tables: true
    views: true
    lua_udfs: true
    virtual_schemas: true
    connections: true
```

## Architecture

```
lineage/
├── backend/                    # Python FastAPI backend
│   ├── app/
│   │   ├── main.py            # FastAPI entry point
│   │   ├── config.py          # Application settings
│   │   ├── models/
│   │   │   ├── domain.py      # Core models (DatabaseObject, TableLevelDependency)
│   │   │   └── api.py         # API request/response schemas
│   │   ├── services/
│   │   │   ├── graph_engine.py    # BFS traversal engine
│   │   │   └── cache_loader.py    # JSON cache loader
│   │   └── routers/
│   │       ├── objects.py     # /objects endpoints
│   │       ├── lineage.py     # /lineage endpoints
│   │       └── search.py      # /search, /schemas, /statistics
│   ├── data/
│   │   └── lineage_cache.json # Pre-computed lineage cache
│   └── scripts/
│       ├── generate_sample_data.py   # Sample data generator
│       ├── extract_from_exasol.py    # Exasol extractor
│       └── script_parser.py          # AST-based SQL/Lua parser
├── frontend/                   # React + TypeScript frontend
│   ├── src/
│   │   ├── App.tsx            # Main application
│   │   ├── components/
│   │   │   ├── LineageGraph/  # React Flow container
│   │   │   ├── Nodes/         # Custom node components
│   │   │   ├── Controls/      # Search, ControlBar
│   │   │   └── Sidebar/       # Object details panel
│   │   ├── hooks/
│   │   │   └── useGraphLayout.ts  # Dagre layout hook
│   │   ├── store/
│   │   │   └── graphStore.ts  # Zustand state management
│   │   ├── services/
│   │   │   └── api.ts         # API client
│   │   └── types/
│   │       └── lineage.ts     # TypeScript interfaces
│   └── package.json
└── docker-compose.yml
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/objects` | List objects (paginated) |
| `GET /api/v1/objects/{id}` | Get object details |
| `GET /api/v1/lineage/{id}/full` | Get full lineage graph (upstream + downstream) |
| `GET /api/v1/lineage/{id}/forward` | Get downstream dependencies |
| `GET /api/v1/lineage/{id}/backward` | Get upstream dependencies |
| `GET /api/v1/search?q=term` | Search objects by name/schema |
| `GET /api/v1/schemas` | List all schemas |
| `GET /api/v1/types` | List all object types |
| `GET /api/v1/statistics` | Cache statistics |
| `GET /health` | Health check |

### API Parameters

**Lineage endpoints:**
- `upstream_depth` (default: 2, max: 10) - How many levels upstream to fetch
- `downstream_depth` (default: 2, max: 10) - How many levels downstream to fetch

**Search endpoint:**
- `q` - Search query (required, min 1 char)
- `limit` (default: 20, max: 100) - Number of results
- `schema` - Filter by schema name
- `type` - Filter by object type (TABLE, VIEW, LUA_UDF, VIRTUAL_SCHEMA, CONNECTION)

## Usage Guide

### Search and Select Object
1. Type in the search bar to find objects
2. Results show object name, schema, and type
3. Click or press Enter to load lineage for that object

### Explore Lineage
1. The selected object appears in the center
2. **Upstream** (left): Objects this one depends on
3. **Downstream** (right): Objects that depend on this one
4. Click `+` buttons on nodes to expand in that direction

### View Object Details
- Click any node to see its properties in the sidebar:
  - Schema and name
  - Object type and owner
  - Created/modified dates
  - Row count and size (for tables)
  - View definition (for views)
  - UDF parameters and script (for Lua UDFs)

### Export Lineage
- **CSV Export**: Click "CSV" button to export lineage relationships to CSV/Excel format
  - Columns: Source Schema, Source Name, Source Type, Target Schema, Target Name, Target Type, Dependency Type
- **PNG Export**: Click "PNG" button to export the current graph as an image

### Navigation Controls
- **Pan**: Click and drag on empty space
- **Zoom**: Scroll wheel or use +/- controls
- **Fit**: Click the fit button to see all nodes
- **Reset**: Returns to initial view (root object only)
- **Layout**: Toggle between horizontal (LR) and vertical (TB)

## Tech Stack

### Backend
- **FastAPI** - High-performance async Python web framework
- **Pydantic** - Data validation and serialization
- **sqlglot** - SQL AST parsing for dependency extraction
- **luaparser** - Lua AST parsing for UDF analysis
- **pyexasol** - Exasol database connectivity

### Frontend
- **React 19** - UI framework
- **TypeScript** - Type-safe JavaScript
- **@xyflow/react (React Flow v12)** - Interactive graph visualization
- **Zustand** - Lightweight state management
- **Dagre** - Hierarchical graph layout algorithm
- **html-to-image** - PNG export functionality
- **Vite** - Fast build tooling

## Data Model

### DatabaseObject
```typescript
{
  id: string;              // "SCHEMA.NAME"
  schema: string;          // Schema name
  name: string;            // Object name
  type: ObjectType;        // TABLE | VIEW | LUA_UDF | VIRTUAL_SCHEMA | CONNECTION
  owner: string;
  object_id: number;
  created_at: string;
  modified_at?: string;
  description?: string;

  // View-specific
  definition?: string;     // SQL view definition

  // UDF-specific
  udf_type?: string;       // SCALAR | SET
  input_parameters?: Array<{name: string, type: string}>;
  output_columns?: Array<{name: string, type: string}>;
  script_language?: string;
  script_text?: string;

  // Virtual schema-specific
  adapter_name?: string;
  connection_name?: string;
  remote_schema?: string;

  // Statistics
  row_count?: number;
  size_bytes?: number;
}
```

### TableLevelDependency
```typescript
{
  source_id: string;       // Object that provides data
  target_id: string;       // Object that consumes data
  dependency_type: string; // VIEW | ETL | CONNECTION | UDF_INPUT | UDF_OUTPUT
  reference_type: string;  // SELECT | INSERT_SELECT | USES | PARAMETER
}
```

## Customization

### Adding New Object Types

1. **Backend**: Add type to `ObjectType` enum in `backend/app/models/domain.py`
2. **Frontend**: Add color mapping in `frontend/src/components/Nodes/LineageNode.tsx`
3. **Sample Data**: Update `generate_sample_data.py` to include new types

### Modifying Node Appearance

Edit `frontend/src/components/Nodes/LineageNode.tsx` and `LineageNode.css`:
- Change colors in the `typeColors` map
- Modify node structure in the component
- Update CSS for styling

### Adjusting Layout

Edit `frontend/src/hooks/useGraphLayout.ts`:
- `nodesep`: Vertical spacing between nodes
- `ranksep`: Horizontal spacing between ranks
- `rankdir`: Layout direction ('LR' or 'TB')

### Adding New API Endpoints

1. Create or edit router in `backend/app/routers/`
2. Add methods to `LineageGraphEngine` in `backend/app/services/graph_engine.py`
3. Update API models in `backend/app/models/api.py`
4. Add frontend API calls in `frontend/src/services/api.ts`

## Performance

The system is optimized for large environments (5000+ objects):

- **Pre-computed Cache**: All lineage data loaded into memory at startup
- **Adjacency Lists**: O(1) neighbor lookups for graph traversal
- **BFS Traversal**: Efficient depth-limited expansion with O(V+E) complexity
- **Lazy Loading**: Expand nodes on-demand via +/- buttons
- **React Flow**: Built-in virtualization renders only visible nodes
- **Indexed Search**: Objects indexed by schema and type for fast filtering

## Troubleshooting

### Backend Issues

**Cache file not found**
```bash
cd backend
python scripts/generate_sample_data.py
```

**Port already in use**
```bash
pkill -f "uvicorn app.main"
uvicorn app.main:app --port 8000
```

### Frontend Issues

**Module not found errors**
```bash
cd frontend
rm -rf node_modules
npm install
```

**CORS errors**
- Ensure backend is running on port 8000
- Check `allow_origins` in `backend/app/main.py`

### Export Issues

**PNG export fails**
- The html-to-image package is required
- Ensure `npm install` completed successfully

## Development

### Backend Development
```bash
cd backend
source ../.lineage/bin/activate
uvicorn app.main:app --reload --port 8000
```

The `--reload` flag enables hot reloading for development.

### Frontend Development
```bash
cd frontend
npm run dev
```

Vite provides hot module replacement (HMR).

### Running Both Together
```bash
# Terminal 1 - Backend
cd backend && source ../.lineage/bin/activate && uvicorn app.main:app --reload --port 8000

# Terminal 2 - Frontend
cd frontend && npm run dev
```

### Building for Production

```bash
# Build frontend
cd frontend
npm run build

# The built files are in frontend/dist/
```

## Requirements

- Python 3.9+
- Node.js 18+
- For Exasol extraction: pyexasol library

## License

MIT
