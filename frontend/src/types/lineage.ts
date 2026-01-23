// Database object types
export type ObjectType =
  | 'TABLE'
  | 'VIEW'
  | 'LUA_UDF'
  | 'VIRTUAL_SCHEMA'
  | 'CONNECTION'
  // BigQuery types
  | 'BIGQUERY_TABLE'
  | 'BIGQUERY_VIEW'
  | 'BIGQUERY_UDF'
  | 'BIGQUERY_PROCEDURE'
  // Composer types
  | 'COMPOSER_DAG';

// Platform types
export type Platform = 'exasol' | 'bigquery' | 'composer';

export interface ColumnInfo {
  name: string;
  data_type: string;
  ordinal_position?: number;
  is_nullable?: boolean;
  is_primary_key?: boolean;
  description?: string;
}

export interface DatabaseObject {
  id: string;
  schema: string;
  name: string;
  type: ObjectType;
  platform?: Platform;  // 'exasol' or 'bigquery'
  owner: string;
  object_id: number;
  created_at: string;
  modified_at?: string;
  description?: string;
  definition?: string;
  udf_type?: string;
  input_parameters?: Array<{ name: string; data_type: string }>;
  output_columns?: Array<{ name: string; data_type: string }>;
  script_language?: string;
  adapter_name?: string;
  connection_name?: string;
  remote_schema?: string;
  connection_string?: string;
  row_count?: number;
  size_bytes?: number;
  columns?: ColumnInfo[];
}

export interface TableLevelDependency {
  source_id: string;
  target_id: string;
  dependency_type: string;
  reference_type: string;
}

// Column-level lineage types
export type TransformationType =
  | 'DIRECT'
  | 'AGGREGATE'
  | 'EXPRESSION'
  | 'CASE'
  | 'CAST'
  | 'FUNCTION'
  | 'UNKNOWN';

export interface ColumnLevelDependency {
  source_object_id: string;
  source_column: string;
  target_object_id: string;
  target_column: string;
  transformation?: string;
  transformation_type: TransformationType;
}

export interface ColumnSourceInfo {
  object_id: string;
  column: string;
  transformation?: string;
  transformation_type: TransformationType;
}

export interface ColumnTargetInfo {
  object_id: string;
  column: string;
}

export interface ColumnLineageResponse {
  object_id: string;
  column_name: string;
  dependencies: ColumnLevelDependency[];
  source_columns: ColumnSourceInfo[];
  target_columns: ColumnTargetInfo[];
}

export interface ObjectColumnLineageResponse {
  object_id: string;
  columns_with_lineage: string[];
  column_lineage: Record<string, ColumnLineageResponse>;
  has_column_lineage: boolean;
}

export interface LineageResponse {
  root_object: DatabaseObject;
  nodes: Record<string, DatabaseObject>;
  edges: TableLevelDependency[];
  has_more_upstream: Record<string, boolean>;
  has_more_downstream: Record<string, boolean>;
}

export interface SearchResult {
  id: string;
  schema: string;
  name: string;
  type: string;
  description?: string;
}

export interface Statistics {
  total_objects: number;
  total_dependencies: number;
  schemas: number;
  tables: number;
  views: number;
  udfs: number;
  virtual_schemas: number;
  connections: number;
  cache_loaded_at?: string;
}
