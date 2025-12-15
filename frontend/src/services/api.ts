import type {
  DatabaseObject,
  LineageResponse,
  SearchResult,
  Statistics,
} from '../types/lineage';

// Use relative URL in production (Docker), full URL in development
const API_BASE = import.meta.env.PROD ? '/api/v1' : 'http://localhost:8000/api/v1';

class ApiClient {
  private async fetch<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status} ${response.statusText}`);
    }

    return response.json();
  }

  // Objects
  async getObject(objectId: string): Promise<DatabaseObject> {
    return this.fetch<DatabaseObject>(`/objects/${encodeURIComponent(objectId)}`);
  }

  async listObjects(params?: {
    page?: number;
    page_size?: number;
    schema?: string;
    type?: string;
  }): Promise<{
    items: DatabaseObject[];
    total: number;
    page: number;
    page_size: number;
    total_pages: number;
  }> {
    const searchParams = new URLSearchParams();
    if (params?.page) searchParams.set('page', params.page.toString());
    if (params?.page_size) searchParams.set('page_size', params.page_size.toString());
    if (params?.schema) searchParams.set('schema', params.schema);
    if (params?.type) searchParams.set('type', params.type);

    const query = searchParams.toString();
    return this.fetch(`/objects${query ? `?${query}` : ''}`);
  }

  // Lineage
  async getFullLineage(
    objectId: string,
    options?: {
      upstreamDepth?: number;
      downstreamDepth?: number;
    }
  ): Promise<LineageResponse> {
    const searchParams = new URLSearchParams();
    if (options?.upstreamDepth !== undefined) {
      searchParams.set('upstream_depth', options.upstreamDepth.toString());
    }
    if (options?.downstreamDepth !== undefined) {
      searchParams.set('downstream_depth', options.downstreamDepth.toString());
    }

    const query = searchParams.toString();
    return this.fetch<LineageResponse>(
      `/lineage/${encodeURIComponent(objectId)}/full${query ? `?${query}` : ''}`
    );
  }

  async getForwardLineage(
    objectId: string,
    options?: { depth?: number }
  ): Promise<LineageResponse> {
    const searchParams = new URLSearchParams();
    if (options?.depth !== undefined) {
      searchParams.set('depth', options.depth.toString());
    }

    const query = searchParams.toString();
    return this.fetch<LineageResponse>(
      `/lineage/${encodeURIComponent(objectId)}/forward${query ? `?${query}` : ''}`
    );
  }

  async getBackwardLineage(
    objectId: string,
    options?: { depth?: number }
  ): Promise<LineageResponse> {
    const searchParams = new URLSearchParams();
    if (options?.depth !== undefined) {
      searchParams.set('depth', options.depth.toString());
    }

    const query = searchParams.toString();
    return this.fetch<LineageResponse>(
      `/lineage/${encodeURIComponent(objectId)}/backward${query ? `?${query}` : ''}`
    );
  }

  // Search
  async search(
    query: string,
    options?: {
      limit?: number;
      schema?: string;
      type?: string;
    }
  ): Promise<SearchResult[]> {
    const searchParams = new URLSearchParams({ q: query });
    if (options?.limit) searchParams.set('limit', options.limit.toString());
    if (options?.schema) searchParams.set('schema', options.schema);
    if (options?.type) searchParams.set('type', options.type);

    return this.fetch<SearchResult[]>(`/search?${searchParams.toString()}`);
  }

  // Metadata
  async getSchemas(): Promise<string[]> {
    return this.fetch<string[]>('/schemas');
  }

  async getTypes(): Promise<string[]> {
    return this.fetch<string[]>('/types');
  }

  async getStatistics(): Promise<Statistics> {
    return this.fetch<Statistics>('/statistics');
  }
}

export const api = new ApiClient();
