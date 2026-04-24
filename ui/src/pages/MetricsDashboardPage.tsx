import React, { useState, useEffect, useCallback } from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  FormControl,
  Select,
  MenuItem,
  CircularProgress,
  Alert,
  IconButton,
  TextField,
  InputAdornment,
  Tooltip,
  Button,
} from "@mui/material";
import {
  Close as CloseIcon,
  Search as SearchIcon,
  BugReport as IssueIcon,
  Explore as ExploreIcon,
} from "@mui/icons-material";
import {
  AreaChart,
  Area,
  ResponsiveContainer,
} from "recharts";
import { useNavigate } from "react-router-dom";
import { useProject } from "../contexts/ProjectContext";
import {
  DashboardMetricData,
  computeDashboardMetrics,
  removeDashboardMetric,
} from "../api/dataSources";

const PERIOD_OPTIONS = [
  { value: 7, label: "Last 7 days" },
  { value: 30, label: "Last 30 days" },
  { value: 90, label: "Last 90 days" },
];

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

export default function MetricsDashboardPage() {
  const { currentProject } = useProject();
  const navigate = useNavigate();

  const [periodDays, setPeriodDays] = useState(30);
  const [metrics, setMetrics] = useState<DashboardMetricData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchMetrics = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    setError(null);
    try {
      const res = await computeDashboardMetrics(currentProject.id, periodDays);
      setMetrics(res.metrics);
    } catch {
      setError("Failed to load dashboard metrics");
    } finally {
      setLoading(false);
    }
  }, [currentProject, periodDays]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const handleRemove = async (dashboardMetricId: string) => {
    if (!currentProject) return;
    try {
      await removeDashboardMetric(currentProject.id, dashboardMetricId);
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
        <Typography variant="h4">Metrics</Typography>
        <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <Select
              value={periodDays}
              onChange={(e) => setPeriodDays(e.target.value as number)}
            >
              {PERIOD_OPTIONS.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
              ))}
            </Select>
          </FormControl>
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
              periodDays={periodDays}
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

function buildDateRange(periodDays: number): string[] {
  const dates: string[] = [];
  const now = new Date();
  for (let i = periodDays - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    dates.push(d.toISOString().slice(0, 10));
  }
  return dates;
}

function MetricTile({
  metric,
  periodDays,
  onRemove,
}: {
  metric: DashboardMetricData;
  periodDays: number;
  onRemove: () => void;
}) {
  const valueByDate = new Map(
    metric.sparkline.map((pt) => [
      pt.date,
      metric.data_type === "currency" ? pt.value / 100 : pt.value,
    ]),
  );
  const dateRange = buildDateRange(periodDays);
  const sparklineData = dateRange.map((date) => ({
    date,
    value: valueByDate.get(date) ?? 0,
  }));

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

        {/* Source and metric name */}
        <Typography
          noWrap
          sx={{
            pr: 3,
            fontWeight: 600,
            fontSize: "0.75rem",
            textTransform: "uppercase",
            letterSpacing: "0.05em",
            color: "text.secondary",
          }}
        >
          {metric.source_name} &middot; {metric.name}
        </Typography>

        {/* Current value */}
        <Typography
          variant="h4"
          sx={{ fontWeight: 700, fontFamily: "monospace", mb: 1 }}
        >
          {formatMetricValue(metric.current_value, metric.data_type)}
        </Typography>

        {/* Sparkline */}
        <Box sx={{ mx: -1 }}>
          <ResponsiveContainer width="100%" height={48}>
            <AreaChart data={sparklineData}>
              <defs>
                <linearGradient id={`grad-${metric.metric_key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#1565c0" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#1565c0" stopOpacity={0} />
                </linearGradient>
              </defs>
              <Area
                type="monotone"
                dataKey="value"
                stroke="#1565c0"
                strokeWidth={1.5}
                fill={`url(#grad-${metric.metric_key})`}
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
}
