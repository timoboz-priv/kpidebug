import apiClient from "./client";

export interface Dimension {
  name: string;
  type: "temporal" | "categorical";
}

export interface MetricDescriptor {
  key: string;
  name: string;
  description: string;
  dimensions: Dimension[];
}

export interface TableColumn {
  key: string;
  name: string;
  description: string;
  type: "string" | "number" | "currency" | "datetime" | "boolean";
  is_primary_key: boolean;
}

export interface TableDescriptor {
  key: string;
  name: string;
  description: string;
  columns: TableColumn[];
}

export interface DataSource {
  id: string;
  project_id: string;
  name: string;
  type: string;
  dimensions: Dimension[];
}

export interface ConnectRequest {
  name: string;
  source_type: string;
  credentials: Record<string, string>;
}

export async function listDataSources(projectId: string): Promise<DataSource[]> {
  const response = await apiClient.get<DataSource[]>(
    `/api/projects/${projectId}/data-sources`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function connectDataSource(
  projectId: string,
  data: ConnectRequest,
): Promise<DataSource> {
  const response = await apiClient.post<DataSource>(
    `/api/projects/${projectId}/data-sources`,
    data,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function disconnectDataSource(
  projectId: string,
  sourceId: string,
): Promise<void> {
  await apiClient.delete(
    `/api/projects/${projectId}/data-sources/${sourceId}`,
    { headers: { "X-Project-Id": projectId } },
  );
}

export async function discoverMetrics(
  projectId: string,
  sourceId: string,
): Promise<MetricDescriptor[]> {
  const response = await apiClient.get<MetricDescriptor[]>(
    `/api/projects/${projectId}/data-sources/${sourceId}/metrics`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function discoverTables(
  projectId: string,
  sourceId: string,
): Promise<TableDescriptor[]> {
  const response = await apiClient.get<TableDescriptor[]>(
    `/api/projects/${projectId}/data-sources/${sourceId}/tables`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

// --- Metric Explorer ---

export interface DimensionFilter {
  dimension: string;
  value: string;
}

export interface MetricExploreRequest {
  source_id: string;
  metric_key: string;
  aggregation: string;
  group_by?: string | null;
  filters?: DimensionFilter[];
  start_time?: string | null;
  end_time?: string | null;
}

export interface ExploreResultRow {
  value: number;
  group: string | null;
}

export interface MetricExploreResponse {
  metric_key: string;
  aggregation: string;
  results: ExploreResultRow[];
  record_count: number;
}

export async function exploreMetric(
  projectId: string,
  data: MetricExploreRequest,
): Promise<MetricExploreResponse> {
  const response = await apiClient.post<MetricExploreResponse>(
    `/api/projects/${projectId}/metrics/explore`,
    data,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

// --- Table Explorer ---

export interface TableFilter {
  column: string;
  operator: string;
  value: string;
}

export interface TableQueryRequest {
  source_id: string;
  table: string;
  filters?: TableFilter[];
  sort_by?: string | null;
  sort_order?: string;
  limit?: number;
  offset?: number;
}

export interface TableQueryResponse {
  table: string;
  columns: TableColumn[];
  rows: Record<string, unknown>[];
  total_count: number;
  from_cache: boolean;
}

export interface SyncResponse {
  table: string;
  row_count: number;
}

export async function queryTable(
  projectId: string,
  data: TableQueryRequest,
): Promise<TableQueryResponse> {
  const response = await apiClient.post<TableQueryResponse>(
    `/api/projects/${projectId}/data/query`,
    data,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function syncTable(
  projectId: string,
  sourceId: string,
  table: string,
): Promise<SyncResponse> {
  const response = await apiClient.post<SyncResponse>(
    `/api/projects/${projectId}/data/sync`,
    { source_id: sourceId, table },
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}
