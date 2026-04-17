import apiClient from "./client";

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

// --- Sync ---

export interface SyncResponse {
  tables?: Record<string, number> | null;
  table?: string;
  row_count?: number;
}

export async function syncSource(
  projectId: string,
  sourceId: string,
): Promise<SyncResponse> {
  const response = await apiClient.post<SyncResponse>(
    `/api/projects/${projectId}/data-sources/${sourceId}/sync`,
    {},
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function syncTable(
  projectId: string,
  sourceId: string,
  tableKey: string,
): Promise<SyncResponse> {
  const response = await apiClient.post<SyncResponse>(
    `/api/projects/${projectId}/data-sources/${sourceId}/tables/${tableKey}/sync`,
    {},
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

// --- Table Query ---

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
