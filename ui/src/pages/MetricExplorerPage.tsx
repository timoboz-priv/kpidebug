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
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Alert,
  CircularProgress,
  Divider,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  IconButton,
} from "@mui/material";
import {
  PlayArrow as RunIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
} from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import {
  DataSource,
  MetricDescriptor,
  MetricExploreResponse,
  DimensionFilter,
  listDataSources,
  discoverMetrics,
  exploreMetric,
} from "../api/dataSources";

const AGGREGATION_METHODS = [
  { value: "sum", label: "Sum" },
  { value: "count", label: "Count" },
  { value: "avg", label: "Average" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
];

const TIME_RANGE_PRESETS = [
  { value: "7d", label: "Last 7 days" },
  { value: "14d", label: "Last 14 days" },
  { value: "30d", label: "Last 30 days" },
  { value: "90d", label: "Last 90 days" },
  { value: "6m", label: "Last 6 months" },
  { value: "1y", label: "Last year" },
  { value: "all", label: "All time" },
  { value: "custom", label: "Custom range" },
];

function resolveTimeRange(
  preset: string,
  customStart: string,
  customEnd: string,
): { start: string | null; end: string | null } {
  if (preset === "all") return { start: null, end: null };
  if (preset === "custom") {
    return { start: customStart || null, end: customEnd || null };
  }
  const now = new Date();
  const start = new Date(now);
  if (preset.endsWith("d")) {
    start.setDate(start.getDate() - parseInt(preset));
  } else if (preset.endsWith("m")) {
    start.setMonth(start.getMonth() - parseInt(preset));
  } else if (preset.endsWith("y")) {
    start.setFullYear(start.getFullYear() - parseInt(preset));
  }
  return { start: start.toISOString(), end: now.toISOString() };
}

const PANEL_WIDTH = 260;

interface SourceWithMetrics {
  source: DataSource;
  metrics: MetricDescriptor[];
}

export default function MetricExplorerPage() {
  const { currentProject } = useProject();
  const [sources, setSources] = useState<SourceWithMetrics[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
  const [selectedMetric, setSelectedMetric] = useState<MetricDescriptor | null>(null);

  const [aggregation, setAggregation] = useState("sum");
  const [groupBy, setGroupBy] = useState<string>("");
  const [filters, setFilters] = useState<DimensionFilter[]>([]);
  const [timeRange, setTimeRange] = useState("30d");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const [result, setResult] = useState<MetricExploreResponse | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);

  const fetchSources = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    try {
      const srcList = await listDataSources(currentProject.id);
      const withMetrics: SourceWithMetrics[] = [];
      for (const source of srcList) {
        try {
          const metrics = await discoverMetrics(currentProject.id, source.id);
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

  useEffect(() => {
    fetchSources();
  }, [fetchSources]);

  const handleMetricSelect = (sourceId: string, metric: MetricDescriptor) => {
    setSelectedSourceId(sourceId);
    setSelectedMetric(metric);
    setResult(null);
    setQueryError(null);
    setFilters([]);
    setGroupBy("");
    setAggregation("sum");
    setTimeRange("30d");
    setCustomStart("");
    setCustomEnd("");
  };

  const handleAddFilter = () => {
    setFilters([...filters, { dimension: "", value: "" }]);
  };

  const handleUpdateFilter = (index: number, filter: DimensionFilter) => {
    const updated = [...filters];
    updated[index] = filter;
    setFilters(updated);
  };

  const handleRemoveFilter = (index: number) => {
    setFilters(filters.filter((_, i) => i !== index));
  };

  const handleRun = async () => {
    if (!currentProject || !selectedMetric || !selectedSourceId) return;
    setQueryLoading(true);
    setQueryError(null);
    try {
      const validFilters = filters.filter((f) => f.dimension && f.value);
      const { start, end } = resolveTimeRange(timeRange, customStart, customEnd);
      const res = await exploreMetric(currentProject.id, {
        source_id: selectedSourceId,
        metric_key: selectedMetric.key,
        aggregation,
        group_by: groupBy || null,
        filters: validFilters.length > 0 ? validFilters : undefined,
        start_time: start,
        end_time: end,
      });
      setResult(res);
    } catch (err: any) {
      setQueryError(err.response?.data?.detail || "Query failed");
    } finally {
      setQueryLoading(false);
    }
  };

  if (!currentProject) return null;

  const categoricalDimensions = selectedMetric
    ? selectedMetric.dimensions.filter((d) => d.type === "categorical")
    : [];

  return (
    <Box sx={{ display: "flex", height: "calc(100vh - 48px)" }}>
      {/* Left panel: metric tree */}
      <Box
        sx={{
          width: PANEL_WIDTH,
          flexShrink: 0,
          borderRight: 1,
          borderColor: "divider",
          overflowY: "auto",
          bgcolor: "background.paper",
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
            <Typography variant="body2" color="text.secondary">
              No data sources connected. Go to Project Settings to add one.
            </Typography>
          </Box>
        ) : (
          <List dense disablePadding>
            {sources.map(({ source, metrics }) => (
              <React.Fragment key={source.id}>
                <ListSubheader
                  sx={{
                    lineHeight: "32px",
                    bgcolor: "background.paper",
                    fontWeight: 600,
                    fontSize: "0.75rem",
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    color: "text.secondary",
                  }}
                >
                  {source.name}
                </ListSubheader>
                {metrics.map((metric) => (
                  <ListItemButton
                    key={metric.key}
                    selected={
                      selectedMetric?.key === metric.key && selectedSourceId === source.id
                    }
                    onClick={() => handleMetricSelect(source.id, metric)}
                    sx={{
                      py: 0.5,
                      pl: 3,
                      borderRadius: 0,
                      "&.Mui-selected": {
                        bgcolor: "primary.main",
                        color: "white",
                        "&:hover": { bgcolor: "primary.dark" },
                      },
                    }}
                  >
                    <ListItemText
                      primary={metric.key}
                      slotProps={{
                        primary: {
                          variant: "body2",
                          noWrap: true,
                          sx: { fontFamily: "monospace", fontSize: "0.8rem" },
                        },
                      }}
                    />
                  </ListItemButton>
                ))}
              </React.Fragment>
            ))}
          </List>
        )}
      </Box>

      {/* Right area */}
      <Box sx={{ flex: 1, overflowY: "auto", p: 3 }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {!selectedMetric ? (
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              height: "100%",
              color: "text.secondary",
            }}
          >
            <Typography variant="body1">Select a metric from the left to explore</Typography>
          </Box>
        ) : (
          <>
            {/* Metric metadata */}
            <Card sx={{ mb: 2 }}>
              <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
                <Box sx={{ display: "flex", alignItems: "baseline", gap: 1, mb: 0.5 }}>
                  <Typography variant="h6" sx={{ fontFamily: "monospace" }}>
                    {selectedMetric.key}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {selectedMetric.name}
                  </Typography>
                </Box>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                  {selectedMetric.description}
                </Typography>
                <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap" }}>
                  {selectedMetric.dimensions.map((d) => (
                    <Chip
                      key={d.name}
                      label={d.name}
                      size="small"
                      variant="outlined"
                      color={d.type === "temporal" ? "primary" : "default"}
                    />
                  ))}
                </Box>
              </CardContent>
            </Card>

            {/* Controls */}
            <Card sx={{ mb: 2 }}>
              <CardContent>
                <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", alignItems: "flex-start" }}>
                  <FormControl size="small" sx={{ minWidth: 140 }}>
                    <InputLabel>Aggregation</InputLabel>
                    <Select
                      value={aggregation}
                      label="Aggregation"
                      onChange={(e) => setAggregation(e.target.value)}
                    >
                      {AGGREGATION_METHODS.map((m) => (
                        <MenuItem key={m.value} value={m.value}>
                          {m.label}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>

                  <FormControl size="small" sx={{ minWidth: 160 }}>
                    <InputLabel>Group by</InputLabel>
                    <Select
                      value={groupBy}
                      label="Group by"
                      onChange={(e) => setGroupBy(e.target.value)}
                    >
                      <MenuItem value="">
                        <em>None</em>
                      </MenuItem>
                      {categoricalDimensions.map((d) => (
                        <MenuItem key={d.name} value={d.name}>
                          {d.name}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>

                  <FormControl size="small" sx={{ minWidth: 160 }}>
                    <InputLabel>Time range</InputLabel>
                    <Select
                      value={timeRange}
                      label="Time range"
                      onChange={(e) => setTimeRange(e.target.value)}
                    >
                      {TIME_RANGE_PRESETS.map((p) => (
                        <MenuItem key={p.value} value={p.value}>
                          {p.label}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  {timeRange === "custom" && (
                    <>
                      <TextField
                        size="small"
                        label="Start"
                        type="datetime-local"
                        value={customStart}
                        onChange={(e) => setCustomStart(e.target.value)}
                        slotProps={{ inputLabel: { shrink: true } }}
                        sx={{ width: 200 }}
                      />
                      <TextField
                        size="small"
                        label="End"
                        type="datetime-local"
                        value={customEnd}
                        onChange={(e) => setCustomEnd(e.target.value)}
                        slotProps={{ inputLabel: { shrink: true } }}
                        sx={{ width: 200 }}
                      />
                    </>
                  )}

                  <Button
                    variant="contained"
                    startIcon={<RunIcon />}
                    onClick={handleRun}
                    disabled={queryLoading}
                    sx={{ alignSelf: "center" }}
                  >
                    {queryLoading ? "Running..." : "Run"}
                  </Button>
                </Box>

                {categoricalDimensions.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
                      <Typography variant="subtitle2">Filters</Typography>
                      <IconButton size="small" onClick={handleAddFilter}>
                        <AddIcon fontSize="small" />
                      </IconButton>
                    </Box>
                    {filters.map((filter, i) => (
                      <Box key={i} sx={{ display: "flex", gap: 1, mb: 1, alignItems: "center" }}>
                        <FormControl size="small" sx={{ minWidth: 160 }}>
                          <InputLabel>Dimension</InputLabel>
                          <Select
                            value={filter.dimension}
                            label="Dimension"
                            onChange={(e) =>
                              handleUpdateFilter(i, { ...filter, dimension: e.target.value })
                            }
                          >
                            {categoricalDimensions.map((d) => (
                              <MenuItem key={d.name} value={d.name}>
                                {d.name}
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                        <TextField
                          size="small"
                          label="Value"
                          value={filter.value}
                          onChange={(e) =>
                            handleUpdateFilter(i, { ...filter, value: e.target.value })
                          }
                          sx={{ width: 200 }}
                        />
                        <IconButton size="small" onClick={() => handleRemoveFilter(i)}>
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Box>
                    ))}
                  </Box>
                )}
              </CardContent>
            </Card>

            {queryError && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {queryError}
              </Alert>
            )}

            {queryLoading && (
              <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                <CircularProgress />
              </Box>
            )}

            {result && !queryLoading && result.record_count === 0 && (
              <Alert severity="info" sx={{ mb: 2 }}>
                No data found for this metric in the selected time range.
              </Alert>
            )}

            {result && !queryLoading && result.record_count > 0 && (
              <ResultsCard result={result} groupBy={groupBy} />
            )}
          </>
        )}
      </Box>
    </Box>
  );
}

function ResultsCard({
  result,
  groupBy,
}: {
  result: MetricExploreResponse;
  groupBy: string;
}) {
  const isGrouped = groupBy && result.results.some((r) => r.group !== null);

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: "flex", alignItems: "baseline", gap: 2, mb: 1 }}>
          <Typography variant="subtitle2" color="text.secondary">
            {result.aggregation.toUpperCase()}({result.metric_key})
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {result.record_count} record{result.record_count !== 1 ? "s" : ""}
          </Typography>
        </Box>

        <Divider sx={{ mb: 2 }} />

        {isGrouped ? (
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>{groupBy}</TableCell>
                  <TableCell align="right">Value</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {result.results.map((row, i) => (
                  <TableRow key={i}>
                    <TableCell>
                      <Chip label={row.group || "(none)"} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell align="right">
                      <Typography variant="body1" sx={{ fontWeight: 600, fontFamily: "monospace" }}>
                        {formatNumber(row.value)}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        ) : (
          <Typography
            variant="h3"
            sx={{ fontWeight: 700, fontFamily: "monospace", textAlign: "center", py: 2 }}
          >
            {formatNumber(result.results[0]?.value ?? 0)}
          </Typography>
        )}
      </CardContent>
    </Card>
  );
}

function formatNumber(value: number): string {
  if (Number.isInteger(value) && Math.abs(value) < 1e15) {
    return value.toLocaleString();
  }
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
