import React, { useEffect, useState, useCallback } from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  List,
  ListItemButton,
  ListItemText,
  ListSubheader,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Alert,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  Checkbox,
  FormControlLabel,
  ToggleButton,
  ToggleButtonGroup,
  Popover,
  TextField,
  IconButton,
  Tooltip,
  Snackbar,
} from "@mui/material";
import {
  Add as AddIcon,
  Close as CloseIcon,
  TableChart as TableIcon,
  ShowChart as ChartIcon,
  DashboardCustomize as DashboardIcon,
  Dashboard as DashboardFilledIcon,
} from "@mui/icons-material";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { useProject } from "../contexts/ProjectContext";
import {
  DataSource,
  MetricDescriptor,
  MetricComputeResponse,
  DashboardMetricEntry,
  TableFilter,
  listDataSources,
  listMetrics,
  computeMetric,
  listDashboardMetrics,
  addDashboardMetric,
  removeDashboardMetric,
} from "../api/dataSources";
import TimeRangeSelector, {
  TimeRange,
  DEFAULT_TIME_RANGE,
  timeRangeToFilters,
} from "../components/TimeRangeSelector";

const PANEL_WIDTH = 260;

const AGGREGATIONS = [
  { value: "sum", label: "Sum" },
  { value: "avg", label: "Average" },
  { value: "avg_daily", label: "Average Daily" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
  { value: "count", label: "Count" },
];

const TIME_BUCKETS = [
  { value: "day", label: "Day" },
  { value: "week", label: "Week" },
  { value: "month", label: "Month" },
  { value: "year", label: "Year" },
];

const CHART_COLORS = [
  "#1565c0", "#e91e63", "#4caf50", "#ff9800", "#9c27b0",
  "#00bcd4", "#795548", "#607d8b", "#f44336", "#3f51b5",
];

interface SourceWithMetrics {
  source: DataSource;
  metrics: MetricDescriptor[];
}

// --- Filter config (simplified for metrics) ---

interface MetricFilter {
  id: string;
  column: string;
  operator: string;
  value: string;
}

let _fid = 0;
function nextId(): string { return `mf${++_fid}`; }

function filterLabel(f: MetricFilter): string {
  const ops: Record<string, string> = {
    eq: "=", neq: "\u2260", gt: ">", gte: "\u2265", lt: "<", lte: "\u2264", contains: "\u2248",
  };
  return `${f.column} ${ops[f.operator] || f.operator} ${f.value}`;
}

export default function MetricsPage() {
  const { currentProject } = useProject();
  const [sources, setSources] = useState<SourceWithMetrics[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [selectedMetric, setSelectedMetric] = useState<MetricDescriptor | null>(null);

  const [groupBy, setGroupBy] = useState<string[]>([]);
  const [aggregation, setAggregation] = useState("sum");
  const [filters, setFilters] = useState<MetricFilter[]>([]);
  const [timeRange, setTimeRange] = useState<TimeRange>(DEFAULT_TIME_RANGE);
  const [timeBucket, setTimeBucket] = useState("day");
  const [viewMode, setViewMode] = useState<"table" | "chart">("table");

  const [result, setResult] = useState<MetricComputeResponse | null>(null);
  const [computing, setComputing] = useState(false);
  const [computeError, setComputeError] = useState<string | null>(null);

  const [filterAnchor, setFilterAnchor] = useState<HTMLElement | null>(null);
  const [editingFilter, setEditingFilter] = useState<MetricFilter | null>(null);

  const [pinnedMetrics, setPinnedMetrics] = useState<DashboardMetricEntry[]>([]);
  const [snackbar, setSnackbar] = useState<string | null>(null);

  const fetchPinned = useCallback(async () => {
    if (!currentProject) return;
    try {
      const entries = await listDashboardMetrics(currentProject.id);
      setPinnedMetrics(entries);
    } catch {
      // non-critical
    }
  }, [currentProject]);

  const isPinned = (metricId: string): DashboardMetricEntry | undefined =>
    pinnedMetrics.find((p) => p.metric_id === metricId);

  const handleTogglePin = async (metricId: string) => {
    if (!currentProject) return;
    const existing = isPinned(metricId);
    try {
      if (existing) {
        await removeDashboardMetric(currentProject.id, existing.id);
        setPinnedMetrics((prev) => prev.filter((p) => p.id !== existing.id));
        setSnackbar("Removed from dashboard");
      } else {
        const entry = await addDashboardMetric(currentProject.id, metricId);
        setPinnedMetrics((prev) => [...prev, entry]);
        setSnackbar("Added to dashboard");
      }
    } catch {
      setSnackbar("Failed to update dashboard");
    }
  };

  const fetchSources = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    try {
      const srcList = await listDataSources(currentProject.id);
      const withMetrics: SourceWithMetrics[] = [];
      for (const source of srcList) {
        try {
          const metrics = await listMetrics(currentProject.id, source.id);
          withMetrics.push({ source, metrics });
        } catch {
          withMetrics.push({ source, metrics: [] });
        }
      }
      setSources(withMetrics);
      setError(null);
    } catch {
      setError("Failed to load data sources");
    } finally {
      setLoading(false);
    }
  }, [currentProject]);

  useEffect(() => { fetchSources(); fetchPinned(); }, [fetchSources, fetchPinned]);

  const handleCompute = useCallback(async () => {
    if (!currentProject || !selectedMetric) return;
    setComputing(true);
    setComputeError(null);
    try {
      const timeCol = "created";
      const timeFilters = timeRangeToFilters(timeCol, timeRange);
      const allFilters: TableFilter[] = [
        ...filters.filter((f) => f.column && f.value).map((f) => ({
          column: f.column,
          operator: f.operator,
          value: f.value,
        })),
        ...timeFilters,
      ];

      const requestGroupBy = [...groupBy];
      const isChart = viewMode === "chart";

      const res = await computeMetric(
        currentProject.id,
        selectedMetric.id,
        {
          group_by: requestGroupBy.length > 0 ? requestGroupBy : undefined,
          aggregation,
          filters: allFilters.length > 0 ? allFilters : undefined,
          time_column: isChart ? timeCol : undefined,
          time_bucket: isChart ? timeBucket : undefined,
        },
      );
      setResult(res);
    } catch (err: any) {
      setComputeError(err.response?.data?.detail || "Computation failed");
    } finally {
      setComputing(false);
    }
  }, [currentProject, selectedMetric, groupBy, aggregation, filters, timeRange, viewMode, timeBucket]);

  useEffect(() => { handleCompute(); }, [handleCompute]);

  const handleMetricSelect = (metric: MetricDescriptor) => {
    setSelectedMetric(metric);
    setGroupBy([]);
    setAggregation("sum");
    setFilters([]);
    setTimeRange(DEFAULT_TIME_RANGE);
    setComputeError(null);
  };

  const toggleDimension = (key: string) => {
    setGroupBy((prev) =>
      prev.includes(key) ? prev.filter((d) => d !== key) : [...prev, key],
    );
  };

  // Filter popover
  const openAddFilter = (e: React.MouseEvent<HTMLElement>) => {
    setEditingFilter({ id: nextId(), column: "", operator: "eq", value: "" });
    setFilterAnchor(e.currentTarget);
  };
  const openEditFilter = (e: React.MouseEvent<HTMLElement>, f: MetricFilter) => {
    setEditingFilter({ ...f });
    setFilterAnchor(e.currentTarget);
  };
  const closeFilterPopover = () => { setFilterAnchor(null); setEditingFilter(null); };
  const saveFilter = () => {
    if (!editingFilter) return;
    setFilters((prev) => {
      const idx = prev.findIndex((f) => f.id === editingFilter.id);
      if (idx >= 0) { const u = [...prev]; u[idx] = editingFilter; return u; }
      return [...prev, editingFilter];
    });
    closeFilterPopover();
  };
  const removeFilter = (id: string) => setFilters((prev) => prev.filter((f) => f.id !== id));

  if (!currentProject) return null;

  return (
    <Box sx={{ display: "flex", height: "calc(100vh - 48px)", overflow: "hidden" }}>
      {/* Left panel */}
      <Box
        sx={{
          width: PANEL_WIDTH, flexShrink: 0, borderRight: 1,
          borderColor: "divider", overflowY: "auto", bgcolor: "background.paper",
        }}
      >
        <Box sx={{ p: 2, pb: 1 }}>
          <Typography variant="h6">Metrics</Typography>
        </Box>

        {loading ? (
          <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
            <CircularProgress size={24} />
          </Box>
        ) : sources.length === 0 ? (
          <Box sx={{ px: 2, py: 3 }}>
            <Typography variant="body2" color="text.secondary">No data sources connected.</Typography>
          </Box>
        ) : (
          <List dense disablePadding>
            {sources.map(({ source, metrics }) => (
              <React.Fragment key={source.id}>
                <ListSubheader sx={{
                  lineHeight: "32px", bgcolor: "background.paper", fontWeight: 600,
                  fontSize: "0.75rem", textTransform: "uppercase",
                  letterSpacing: "0.05em", color: "text.secondary",
                }}>
                  {source.name}
                </ListSubheader>
                {metrics.map((metric) => {
                  const pinned = isPinned(metric.id);
                  return (
                    <ListItemButton
                      key={metric.id}
                      selected={selectedMetric?.id === metric.id}
                      onClick={() => handleMetricSelect(metric)}
                      sx={{
                        py: 0.5, pl: 3, pr: 1, borderRadius: 0,
                        "&.Mui-selected": {
                          bgcolor: "primary.main", color: "white",
                          "&:hover": { bgcolor: "primary.dark" },
                        },
                      }}
                    >
                      <ListItemText primary={metric.name} />
                      <Tooltip title={pinned ? "Remove from dashboard" : "Add to dashboard"}>
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleTogglePin(metric.id);
                          }}
                          sx={{
                            p: 0.4,
                            color: pinned ? "inherit" : "text.disabled",
                          }}
                        >
                          {pinned ? <DashboardFilledIcon fontSize="small" /> : <DashboardIcon fontSize="small" />}
                        </IconButton>
                      </Tooltip>
                    </ListItemButton>
                  );
                })}
              </React.Fragment>
            ))}
          </List>
        )}
      </Box>

      {/* Right area */}
      <Box sx={{ flex: 1, overflow: "auto", p: 3, display: "flex", flexDirection: "column", minWidth: 0 }}>
        {error && <Alert severity="error" sx={{ mb: 2, flexShrink: 0 }}>{error}</Alert>}

        {!selectedMetric ? (
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1, color: "text.secondary" }}>
            <Typography variant="body1">Select a metric from the left</Typography>
          </Box>
        ) : (
          <>
            {/* Metric header */}
            <Card sx={{ mb: 2, flexShrink: 0 }}>
              <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 0.5 }}>
                  <Typography variant="h6">{selectedMetric.name}</Typography>
                  <Chip label={selectedMetric.data_type} size="small" variant="outlined" />
                </Box>
                <Typography variant="body2" color="text.secondary">{selectedMetric.description}</Typography>
              </CardContent>
            </Card>

            {/* Controls */}
            <Card sx={{ mb: 2, flexShrink: 0 }}>
              <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
                {/* Row 1: Dimensions + Aggregation */}
                <Box sx={{ display: "flex", gap: 2, alignItems: "center", flexWrap: "wrap", mb: 1.5 }}>
                  <Typography variant="subtitle2" sx={{ mr: 0.5 }}>Group by:</Typography>
                  {selectedMetric.dimensions.map((d) => (
                    <FormControlLabel
                      key={d.key}
                      control={
                        <Checkbox
                          size="small"
                          checked={groupBy.includes(d.key)}
                          onChange={() => toggleDimension(d.key)}
                        />
                      }
                      label={d.name}
                      sx={{ mr: 0, "& .MuiFormControlLabel-label": { fontSize: "0.85rem" } }}
                    />
                  ))}
                  <Box sx={{ flex: 1 }} />
                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel>Aggregation</InputLabel>
                    <Select value={aggregation} label="Aggregation" onChange={(e) => setAggregation(e.target.value)}>
                      {AGGREGATIONS.map((a) => (
                        <MenuItem key={a.value} value={a.value}>{a.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Box>

                {/* Row 2: Time range + view toggle + filters + run */}
                <Box sx={{ display: "flex", gap: 1, alignItems: "center", flexWrap: "wrap" }}>
                  <TimeRangeSelector value={timeRange} onChange={setTimeRange} compact />

                  {viewMode === "chart" && (
                    <FormControl size="small" sx={{ minWidth: 90 }}>
                      <InputLabel>Aggregation</InputLabel>
                      <Select value={timeBucket} label="Aggregation" onChange={(e) => setTimeBucket(e.target.value)}>
                        {TIME_BUCKETS.map((b) => (
                          <MenuItem key={b.value} value={b.value}>{b.label}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  )}

                  <Box sx={{ display: "flex", gap: 0.5 }}>
                    {filters.map((f) => (
                      <Chip
                        key={f.id}
                        label={filterLabel(f)}
                        size="small"
                        variant="outlined"
                        onClick={(e) => openEditFilter(e, f)}
                        onDelete={() => removeFilter(f.id)}
                        sx={{ cursor: "pointer" }}
                      />
                    ))}
                    <IconButton size="small" onClick={openAddFilter}>
                      <AddIcon fontSize="small" />
                    </IconButton>
                  </Box>

                  <Box sx={{ flex: 1 }} />

                  <ToggleButtonGroup
                    value={viewMode}
                    exclusive
                    onChange={(_, v) => { if (v) setViewMode(v); }}
                    size="small"
                  >
                    <ToggleButton value="table"><TableIcon fontSize="small" /></ToggleButton>
                    <ToggleButton value="chart"><ChartIcon fontSize="small" /></ToggleButton>
                  </ToggleButtonGroup>

                  {computing && <CircularProgress size={20} />}
                </Box>
              </CardContent>
            </Card>

            {/* Filter popover */}
            <Popover
              open={Boolean(filterAnchor)}
              anchorEl={filterAnchor}
              onClose={closeFilterPopover}
              anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
            >
              <Box sx={{ p: 2, width: 280 }}>
                {editingFilter && (
                  <>
                    <Box sx={{ display: "flex", justifyContent: "space-between", mb: 1 }}>
                      <Typography variant="subtitle2">Filter</Typography>
                      <IconButton size="small" onClick={closeFilterPopover}><CloseIcon fontSize="small" /></IconButton>
                    </Box>
                    <TextField fullWidth size="small" label="Column" value={editingFilter.column}
                      onChange={(e) => setEditingFilter({ ...editingFilter, column: e.target.value })} sx={{ mb: 1 }} />
                    <FormControl fullWidth size="small" sx={{ mb: 1 }}>
                      <InputLabel>Operator</InputLabel>
                      <Select value={editingFilter.operator} label="Operator"
                        onChange={(e) => setEditingFilter({ ...editingFilter, operator: e.target.value })}>
                        <MenuItem value="eq">equals</MenuItem>
                        <MenuItem value="neq">not equals</MenuItem>
                        <MenuItem value="gt">greater than</MenuItem>
                        <MenuItem value="gte">greater or equal</MenuItem>
                        <MenuItem value="lt">less than</MenuItem>
                        <MenuItem value="lte">less or equal</MenuItem>
                        <MenuItem value="contains">contains</MenuItem>
                      </Select>
                    </FormControl>
                    <TextField fullWidth size="small" label="Value" value={editingFilter.value}
                      onChange={(e) => setEditingFilter({ ...editingFilter, value: e.target.value })} sx={{ mb: 1.5 }} />
                    <Button variant="contained" fullWidth size="small" onClick={saveFilter}>Apply</Button>
                  </>
                )}
              </Box>
            </Popover>

            {/* Results */}
            {computeError && <Alert severity="error" sx={{ mb: 2, flexShrink: 0 }}>{computeError}</Alert>}

            {computing && (
              <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                <CircularProgress />
              </Box>
            )}

            {result && !computing && (
              viewMode === "chart"
                ? <ChartView result={result} dataType={selectedMetric.data_type} groupBy={groupBy} />
                : <ScalarView result={result} dataType={selectedMetric.data_type} />
            )}
          </>
        )}
      </Box>

      <Snackbar
        open={snackbar !== null}
        autoHideDuration={2000}
        onClose={() => setSnackbar(null)}
        message={snackbar}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      />
    </Box>
  );
}

// --- Scalar / table view ---

function ScalarView({
  result,
  dataType,
}: {
  result: MetricComputeResponse;
  dataType: string;
}) {
  const hasGroups = result.results.length > 0 && Object.keys(result.results[0].groups).length > 0;

  if (!hasGroups) {
    const value = result.results[0]?.value ?? 0;
    return (
      <Card>
        <CardContent sx={{ textAlign: "center", py: 4 }}>
          <Typography variant="h3" sx={{ fontWeight: 700, fontFamily: "monospace" }}>
            {formatMetricValue(value, dataType)}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  const dimKeys = Object.keys(result.results[0].groups);

  return (
    <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", minHeight: 0 }}>
      <TableContainer sx={{ flex: 1, overflow: "auto" }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              {dimKeys.map((k) => (
                <TableCell key={k} sx={{ fontWeight: 600 }}>{k}</TableCell>
              ))}
              <TableCell align="right" sx={{ fontWeight: 600 }}>Value</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {result.results.map((row, i) => (
              <TableRow key={i} hover>
                {dimKeys.map((k) => (
                  <TableCell key={k}>
                    <Chip label={row.groups[k] || "(empty)"} size="small" variant="outlined" />
                  </TableCell>
                ))}
                <TableCell align="right">
                  <Typography variant="body1" sx={{ fontWeight: 600, fontFamily: "monospace" }}>
                    {formatMetricValue(row.value, dataType)}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Card>
  );
}

// --- Chart view ---

function ChartView({
  result,
  dataType,
  groupBy,
}: {
  result: MetricComputeResponse;
  dataType: string;
  groupBy: string[];
}) {
  const results = result.results;
  if (results.length === 0) {
    return (
      <Card><CardContent sx={{ textAlign: "center", py: 4 }}>
        <Typography variant="body2" color="text.secondary">No data</Typography>
      </CardContent></Card>
    );
  }

  const dimKeys = Object.keys(results[0].groups);
  const timeKey = dimKeys.find((k) => k === "created" || k === "date") || dimKeys[0];
  const seriesKeys = dimKeys.filter((k) => k !== timeKey);

  if (seriesKeys.length === 0) {
    const chartData = results.map((r) => ({
      time: r.groups[timeKey] || "",
      value: dataType === "currency" ? r.value / 100 : r.value,
    }));

    return (
      <Card sx={{ flexShrink: 0 }}>
        <CardContent>
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <RechartsTooltip formatter={(v) => formatMetricValue(Number(v), dataType === "currency" ? "number" : dataType)} />
              <Line type="monotone" dataKey="value" stroke={CHART_COLORS[0]} strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    );
  }

  const seriesMap = new Map<string, Map<string, number>>();
  const allTimes = new Set<string>();

  for (const r of results) {
    const time = r.groups[timeKey] || "";
    const seriesLabel = seriesKeys.map((k) => r.groups[k] || "").join(" / ");
    allTimes.add(time);
    if (!seriesMap.has(seriesLabel)) seriesMap.set(seriesLabel, new Map());
    seriesMap.get(seriesLabel)!.set(time, dataType === "currency" ? r.value / 100 : r.value);
  }

  const sortedTimes = Array.from(allTimes).sort();
  const seriesNames = Array.from(seriesMap.keys()).sort();

  const chartData = sortedTimes.map((time) => {
    const point: Record<string, string | number> = { time };
    for (const name of seriesNames) {
      point[name] = seriesMap.get(name)?.get(time) ?? 0;
    }
    return point;
  });

  return (
    <Card sx={{ flexShrink: 0 }}>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <RechartsTooltip />
            <Legend />
            {seriesNames.map((name, i) => (
              <Line
                key={name}
                type="monotone"
                dataKey={name}
                stroke={CHART_COLORS[i % CHART_COLORS.length]}
                strokeWidth={2}
                dot={{ r: 2 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}

// --- Formatting ---

function formatMetricValue(value: number, dataType: string): string {
  if (dataType === "percent") {
    return `${value.toFixed(2)}%`;
  }
  if (dataType === "currency") {
    const major = value / 100;
    return major.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
