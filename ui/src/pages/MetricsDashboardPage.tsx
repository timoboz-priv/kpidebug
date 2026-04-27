import React, { useState, useEffect, useCallback } from "react";
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
} from "recharts";
import { useNavigate } from "react-router-dom";
import { useProject } from "../contexts/ProjectContext";
import {
  DashboardMetricData,
  computeDashboardMetrics,
  removeDashboardMetric,
  processProject,
} from "../api/dataSources";

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

export default function MetricsDashboardPage() {
  const { currentProject } = useProject();
  const navigate = useNavigate();

  const [metrics, setMetrics] = useState<DashboardMetricData[]>([]);
  const [loading, setLoading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchMetrics = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    setError(null);
    try {
      const res = await computeDashboardMetrics(currentProject.id);
      setMetrics(res.metrics);
    } catch {
      setError("Failed to load dashboard metrics");
    } finally {
      setLoading(false);
    }
  }, [currentProject]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const handleRefresh = async () => {
    if (!currentProject) return;
    setProcessing(true);
    setError(null);
    try {
      await processProject(currentProject.id);
      await fetchMetrics();
    } catch {
      setError("Failed to refresh data");
    } finally {
      setProcessing(false);
    }
  };

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

function MetricTile({
  metric,
  onRemove,
}: {
  metric: DashboardMetricData;
  onRemove: () => void;
}) {
  const valueByDate = new Map(
    metric.sparkline.map((pt) => [
      pt.date,
      metric.data_type === "currency" ? pt.value / 100 : pt.value,
    ]),
  );
  const dateRange = metric.sparkline.map((pt) => pt.date);
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
          {metric.source_name ? `${metric.source_name} · ` : ""}{metric.name}
        </Typography>

        {/* Current value */}
        <Typography
          variant="h4"
          sx={{ fontWeight: 700, fontFamily: "monospace", mb: 0.5 }}
        >
          {formatMetricValue(metric.current_value, metric.data_type)}
        </Typography>

        {/* Change badges */}
        <Box sx={{ display: "flex", gap: 1.5, mb: 1 }}>
          <ChangeBadge label="1D" value={metric.change_1d} />
          <ChangeBadge label="3D" value={metric.change_3d} />
          <ChangeBadge label="7D" value={metric.change_7d} />
        </Box>

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

function ChangeBadge({ label, value }: { label: string; value: number }) {
  return (
    <Box sx={{ display: "flex", alignItems: "baseline", gap: 0.3 }}>
      <Typography
        sx={{
          fontSize: "0.65rem",
          fontWeight: 600,
          color: "text.disabled",
          textTransform: "uppercase",
        }}
      >
        {label}
      </Typography>
      <Typography
        sx={{
          fontSize: "0.75rem",
          fontWeight: 600,
          color: changeColor(value),
          fontFamily: "monospace",
        }}
      >
        {formatChange(value)}
      </Typography>
    </Box>
  );
}
