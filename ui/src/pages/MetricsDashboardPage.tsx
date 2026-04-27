import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  CircularProgress,
  Alert,
  IconButton,
  TextField,
  InputAdornment,
  Tooltip,
  Button,
  ToggleButton,
  ToggleButtonGroup,
} from "@mui/material";
import {
  Close as CloseIcon,
  Search as SearchIcon,
  BugReport as IssueIcon,
  Explore as ExploreIcon,
  Refresh as RefreshIcon,
} from "@mui/icons-material";
import {
  AreaChart,
  Area,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  ReferenceLine,
  XAxis,
} from "recharts";
import { useNavigate } from "react-router-dom";
import { useProject } from "../contexts/ProjectContext";
import {
  DashboardMetricData,
  computeDashboardMetrics,
  removeDashboardMetric,
  processProject,
} from "../api/dataSources";

type TimeWindow = 1 | 3 | 7 | 30;

const SPARKLINE_POINTS: Record<TimeWindow, number> = { 1: 5, 3: 10, 7: 20, 30: 60 };

function formatMetricValue(value: number, dataType: string): string {
  if (dataType === "percent") {
    return `${value.toFixed(1)}%`;
  }
  if (dataType === "currency") {
    const major = value / 100;
    if (major >= 1_000_000) return `$${(major / 1_000_000).toFixed(1)}M`;
    if (major >= 1_000) return `$${(major / 1_000).toFixed(1)}K`;
    return `$${major.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatChange(change: number): string {
  const pct = (change * 100).toFixed(1);
  if (change > 0) return `+${pct}%`;
  if (change < 0) return `${pct}%`;
  return "0.0%";
}

function changeColor(change: number): string {
  if (change > 0) return "#2e7d32";
  if (change < 0) return "#c62828";
  return "#616161";
}

const SOURCE_TYPE_LABELS: Record<string, string> = {
  stripe: "Stripe",
  google_analytics: "Google Analytics",
  custom: "Custom",
};

function formatSourceType(sourceType: string): string {
  return SOURCE_TYPE_LABELS[sourceType] || sourceType;
}

function getValueForWindow(m: DashboardMetricData, w: TimeWindow): number {
  if (w === 1) return m.value_1d;
  if (w === 3) return m.value_3d;
  if (w === 7) return m.value_7d;
  return m.value_30d;
}

function getChangeForWindow(m: DashboardMetricData, w: TimeWindow): number {
  if (w === 1) return m.change_1d;
  if (w === 3) return m.change_3d;
  if (w === 7) return m.change_7d;
  return m.change_30d;
}

export default function MetricsDashboardPage() {
  const { currentProject } = useProject();
  const navigate = useNavigate();

  const [timeWindow, setTimeWindow] = useState<TimeWindow>(1);
  const [metrics, setMetrics] = useState<DashboardMetricData[]>([]);
  const [loading, setLoading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fetchingRef = useRef(false);

  const projectId = currentProject?.id;

  const fetchMetrics = useCallback(async () => {
    if (!projectId || fetchingRef.current) return;
    fetchingRef.current = true;
    setLoading(true);
    setError(null);
    try {
      const res = await computeDashboardMetrics(projectId);
      setMetrics(res.metrics);
    } catch {
      setError("Failed to load dashboard metrics");
    } finally {
      setLoading(false);
      fetchingRef.current = false;
    }
  }, [projectId]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const handleRefresh = async () => {
    if (!projectId) return;
    setProcessing(true);
    setError(null);
    try {
      await processProject(projectId);
      await fetchMetrics();
    } catch {
      setError("Failed to refresh data");
    } finally {
      setProcessing(false);
    }
  };

  const handleRemove = async (dashboardMetricId: string) => {
    if (!projectId) return;
    try {
      await removeDashboardMetric(projectId, dashboardMetricId);
      setMetrics((prev) => prev.filter((m) => m.dashboard_metric_id !== dashboardMetricId));
    } catch {
      setError("Failed to remove metric");
    }
  };

  if (!currentProject) return null;

  return (
    <Box sx={{ p: 3, overflow: "auto", height: "calc(100vh - 48px)" }}>
      {/* Header */}
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
          <Typography variant="h4">Metrics</Typography>
          <ToggleButtonGroup
            value={timeWindow}
            exclusive
            size="small"
            onChange={(_, v) => v !== null && setTimeWindow(v as TimeWindow)}
          >
            <ToggleButton value={1} sx={{ textTransform: "none" }}>1d</ToggleButton>
            <ToggleButton value={3} sx={{ textTransform: "none" }}>3d</ToggleButton>
            <ToggleButton value={7} sx={{ textTransform: "none" }}>7d</ToggleButton>
            <ToggleButton value={30} sx={{ textTransform: "none" }}>30d</ToggleButton>
          </ToggleButtonGroup>
        </Box>
        <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
          <Button
            variant="outlined"
            size="small"
            startIcon={processing ? <CircularProgress size={16} /> : <RefreshIcon />}
            onClick={handleRefresh}
            disabled={processing}
          >
            {processing ? "Processing..." : "Refresh Data"}
          </Button>
          <Button
            variant="outlined"
            size="small"
            startIcon={<ExploreIcon />}
            onClick={() => navigate("/metrics/explorer")}
          >
            Explorer
          </Button>
        </Box>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {/* Metric tiles */}
      {loading ? (
        <Box sx={{ display: "flex", justifyContent: "center", py: 8 }}>
          <CircularProgress />
        </Box>
      ) : metrics.length === 0 ? (
        <Card sx={{ mb: 3 }}>
          <CardContent sx={{ textAlign: "center", py: 6 }}>
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No metrics pinned yet
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Go to the Explorer to browse available metrics and add them to your dashboard.
            </Typography>
            <Button
              variant="contained"
              startIcon={<ExploreIcon />}
              onClick={() => navigate("/metrics/explorer")}
            >
              Open Explorer
            </Button>
          </CardContent>
        </Card>
      ) : (
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
            gap: 2,
            mb: 3,
          }}
        >
          {metrics.map((m) => (
            <MetricTile
              key={m.dashboard_metric_id}
              metric={m}
              timeWindow={timeWindow}
              onRemove={() => handleRemove(m.dashboard_metric_id)}
            />
          ))}
        </Box>
      )}

      {/* Placeholder: Ask a question */}
      <Card sx={{ mb: 2 }}>
        <CardContent sx={{ py: 2, "&:last-child": { pb: 2 } }}>
          <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 1 }}>
            Ask about your metrics
          </Typography>
          <TextField
            fullWidth
            size="small"
            placeholder="e.g. Why did revenue drop last week?"
            disabled
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon color="disabled" fontSize="small" />
                  </InputAdornment>
                ),
              },
            }}
          />
          <Typography variant="caption" color="text.disabled" sx={{ mt: 0.5, display: "block" }}>
            Coming soon
          </Typography>
        </CardContent>
      </Card>

      {/* Placeholder: Detected issues */}
      <Card>
        <CardContent sx={{ py: 2, "&:last-child": { pb: 2 } }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
            <IssueIcon fontSize="small" color="disabled" />
            <Typography variant="subtitle2" color="text.secondary">
              Detected Issues
            </Typography>
          </Box>
          <Typography variant="body2" color="text.disabled">
            No issues detected yet. The system will surface anomalies and root causes here.
          </Typography>
        </CardContent>
      </Card>
    </Box>
  );
}

function SparklineTooltipContent({ active, payload, dataType }: any) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  const value = row.active ?? row.history ?? 0;
  return (
    <Box sx={{ bgcolor: "background.paper", border: 1, borderColor: "divider", borderRadius: 1, px: 1, py: 0.5 }}>
      <Typography sx={{ fontSize: "0.7rem", color: "text.secondary" }}>{row.date}</Typography>
      <Typography sx={{ fontSize: "0.8rem", fontWeight: 600, fontFamily: "monospace" }}>
        {formatMetricValue(value, dataType)}
      </Typography>
    </Box>
  );
}

function MetricTile({
  metric,
  timeWindow,
  onRemove,
}: {
  metric: DashboardMetricData;
  timeWindow: TimeWindow;
  onRemove: () => void;
}) {
  const maxPoints = SPARKLINE_POINTS[timeWindow];
  const allData = metric.sparkline.map((pt) => ({
    date: pt.date,
    value: metric.data_type === "currency" ? pt.value / 100 : pt.value,
  }));
  const sparklineData = allData.slice(-maxPoints);

  const historyCount = Math.max(0, sparklineData.length - timeWindow);
  const boundaryDate = historyCount > 0 ? sparklineData[historyCount].date : "";

  const chartData = sparklineData.map((pt, i) => ({
    date: pt.date,
    history: i <= historyCount ? pt.value : undefined,
    active: i >= historyCount ? pt.value : undefined,
  }));

  const displayValue = getValueForWindow(metric, timeWindow);
  const displayChange = getChangeForWindow(metric, timeWindow);
  const gradId = `grad-${metric.metric_key}`;
  const gradHistId = `grad-hist-${metric.metric_key}`;

  return (
    <Card
      sx={{
        position: "relative",
        "&:hover .remove-btn": { opacity: 1 },
      }}
    >
      <CardContent sx={{ pb: 1, "&:last-child": { pb: 1.5 } }}>
        {/* Remove button */}
        <Tooltip title="Remove from dashboard">
          <IconButton
            className="remove-btn"
            size="small"
            onClick={onRemove}
            sx={{
              position: "absolute",
              top: 8,
              right: 8,
              opacity: 0,
              transition: "opacity 0.15s",
            }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </Tooltip>

        {/* Metric name + aggregation */}
        <Tooltip title={formatSourceType(metric.source_type)} placement="top-start">
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.5, pr: 3 }}>
            <Typography
              noWrap
              sx={{
                fontWeight: 600,
                fontSize: "0.75rem",
                textTransform: "uppercase",
                letterSpacing: "0.05em",
                color: "text.secondary",
                flex: 1,
                minWidth: 0,
              }}
            >
              {metric.name}
            </Typography>
            <Typography
              sx={{
                fontSize: "0.6rem",
                fontWeight: 500,
                color: "text.disabled",
                textTransform: "uppercase",
                whiteSpace: "nowrap",
              }}
            >
              {metric.aggregation === "avg_daily" ? "avg/d" : metric.aggregation}
            </Typography>
          </Box>
        </Tooltip>

        {/* Value + change */}
        <Box sx={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", mb: 0.5 }}>
          <Typography
            variant="h4"
            sx={{ fontWeight: 700, fontFamily: "monospace" }}
          >
            {formatMetricValue(displayValue, metric.data_type)}
          </Typography>
          <Typography
            sx={{
              fontSize: "1.1rem",
              fontWeight: 600,
              fontFamily: "monospace",
              color: changeColor(displayChange),
            }}
          >
            {formatChange(displayChange)}
          </Typography>
        </Box>

        {/* Sparkline */}
        <Box sx={{ mx: -1 }}>
          <ResponsiveContainer width="100%" height={48}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id={gradHistId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#9e9e9e" stopOpacity={0.12} />
                  <stop offset="95%" stopColor="#9e9e9e" stopOpacity={0} />
                </linearGradient>
                <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#1565c0" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#1565c0" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis dataKey="date" hide />
              <RechartsTooltip
                content={<SparklineTooltipContent dataType={metric.data_type} />}
                cursor={{ stroke: "#90caf9", strokeWidth: 1 }}
              />
              {boundaryDate && (
                <ReferenceLine
                  x={boundaryDate}
                  stroke="#bdbdbd"
                  strokeDasharray="3 3"
                  strokeWidth={1}
                />
              )}
              <Area
                type="monotone"
                dataKey="history"
                stroke="#bdbdbd"
                strokeWidth={1.5}
                fill={`url(#${gradHistId})`}
                dot={false}
                isAnimationActive={false}
                connectNulls={false}
              />
              <Area
                type="monotone"
                dataKey="active"
                stroke="#1565c0"
                strokeWidth={1.5}
                fill={`url(#${gradId})`}
                dot={false}
                isAnimationActive={false}
                connectNulls={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
}
