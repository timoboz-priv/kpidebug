import apiClient from "./client";

export interface TableColumn {
  key: string;
  name: string;
  description: string;
  type: "string" | "number" | "currency" | "datetime" | "boolean";
  is_primary_key: boolean;
  annotations: string[];
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

export interface TableSyncError {
  table: string;
  error: string;
}

export interface SyncResponse {
  tables?: Record<string, number> | null;
  table?: string;
  row_count?: number;
  errors?: TableSyncError[];
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

// --- Metrics ---

export interface MetricDimension {
  key: string;
  name: string;
}

export interface MetricDescriptor {
  id: string;
  key: string;
  name: string;
  description: string;
  data_type: string;
  source_type: string;
  default_aggregation: string;
  dimensions: MetricDimension[];
}

export interface MetricComputeResult {
  value: number;
  groups: Record<string, string>;
}

export interface MetricComputeRequest {
  group_by?: string[];
  aggregation?: string;
  filters?: TableFilter[];
  time_column?: string | null;
  time_bucket?: string | null;
}

export interface MetricComputeResponse {
  metric_key: string;
  data_type: string;
  results: MetricComputeResult[];
}

export async function listMetrics(
  projectId: string,
  sourceId?: string,
): Promise<MetricDescriptor[]> {
  const params = sourceId ? { params: { source_id: sourceId } } : {};
  const response = await apiClient.get<MetricDescriptor[]>(
    `/api/projects/${projectId}/metrics`,
    { headers: { "X-Project-Id": projectId }, ...params },
  );
  return response.data;
}

export async function computeMetric(
  projectId: string,
  metricId: string,
  request: MetricComputeRequest,
): Promise<MetricComputeResponse> {
  const response = await apiClient.post<MetricComputeResponse>(
    `/api/projects/${projectId}/metrics/${metricId}/compute`,
    request,
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

// --- Dashboard ---

export interface DashboardMetricEntry {
  id: string;
  project_id: string;
  metric_id: string;
  position: number;
  added_at: string;
}

export interface SparklinePoint {
  date: string;
  value: number;
}

export interface DashboardMetricData {
  dashboard_metric_id: string;
  metric_id: string;
  source_type: string;
  metric_key: string;
  name: string;
  description: string;
  data_type: string;
  aggregation: string;
  current_value: number;
  value_1d: number;
  value_3d: number;
  value_7d: number;
  value_30d: number;
  sparkline: SparklinePoint[];
  change_1d: number;
  change_3d: number;
  change_7d: number;
  change_30d: number;
}

export interface DashboardComputeResponse {
  metrics: DashboardMetricData[];
}

export async function listDashboardMetrics(
  projectId: string,
): Promise<DashboardMetricEntry[]> {
  const response = await apiClient.get<DashboardMetricEntry[]>(
    `/api/projects/${projectId}/dashboard/metrics`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function addDashboardMetric(
  projectId: string,
  metricId: string,
  aggregation: string = "sum",
): Promise<DashboardMetricEntry> {
  const response = await apiClient.post<DashboardMetricEntry>(
    `/api/projects/${projectId}/dashboard/metrics`,
    { metric_id: metricId, aggregation },
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function removeDashboardMetric(
  projectId: string,
  dashboardMetricId: string,
): Promise<void> {
  await apiClient.delete(
    `/api/projects/${projectId}/dashboard/metrics/${dashboardMetricId}`,
    { headers: { "X-Project-Id": projectId } },
  );
}

export async function computeDashboardMetrics(
  projectId: string,
): Promise<DashboardComputeResponse> {
  const response = await apiClient.get<DashboardComputeResponse>(
    `/api/projects/${projectId}/dashboard/metrics/compute`,
    { headers: { "X-Project-Id": projectId } },
  );
  return response.data;
}

export async function processProject(
  projectId: string,
): Promise<void> {
  await apiClient.post(
    `/api/projects/${projectId}/process`,
    {},
    { headers: { "X-Project-Id": projectId } },
  );
}

// --- Insights ---

export interface InsightSignal {
  metric_id: string;
  description: string;
  value: number;
  change: number;
  period_days: number;
}

export interface InsightAction {
  description: string;
  priority: "high" | "medium" | "low";
}

export interface RevenueImpact {
  value: number;
  description: string;
}

export interface InsightCounterfactual {
  value: number;
  metric_id: string;
  metric_name: string;
  description: string;
  revenue_impact: RevenueImpact;
}

export interface InsightConfidence {
  score: number;
  description: string;
}

export interface InsightData {
  id: string;
  headline: string;
  description: string;
  detected_at: string;
  signals: InsightSignal[];
  actions: InsightAction[];
  counterfactual: InsightCounterfactual;
  revenue_impact: RevenueImpact;
  confidence: InsightConfidence;
}

export async function fetchDashboardInsights(
  projectId: string,
  limit: number = 20,
): Promise<InsightData[]> {
  const response = await apiClient.get<InsightData[]>(
    `/api/projects/${projectId}/dashboard/insights`,
    { headers: { "X-Project-Id": projectId }, params: { limit } },
  );
  return response.data;
}
