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
  Chip,
  Collapse,
  LinearProgress,
} from "@mui/material";
import {
  Close as CloseIcon,
  Search as SearchIcon,
  Explore as ExploreIcon,
  Refresh as RefreshIcon,
  ExpandMore as ExpandMoreIcon,
  KeyboardArrowRight as ArrowRightIcon,
  TrendingDown as TrendingDownIcon,
  TrendingUp as TrendingUpIcon,
  Warning as WarningIcon,
  ErrorOutlined as ErrorIcon,
  Info as InfoIcon,
  LightbulbOutlined as LightbulbIcon,
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
  InsightData,
  computeDashboardMetrics,
  fetchDashboardInsights,
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
  const [insights, setInsights] = useState<InsightData[]>([]);
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
      const [metricsRes, insightsRes] = await Promise.all([
        computeDashboardMetrics(projectId),
        fetchDashboardInsights(projectId),
      ]);
      setMetrics(metricsRes.metrics);
      setInsights(insightsRes);
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

      {/* Detected issues */}
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
          <Typography variant="h5">Detected Issues</Typography>
          {insights.length > 0 && (
            <Chip
              label={`${insights.length} active`}
              size="small"
              color="error"
              variant="outlined"
              sx={{ fontWeight: 600, fontSize: "0.75rem" }}
            />
          )}
        </Box>
      </Box>
      {insights.length === 0 ? (
        <Card sx={{ mb: 3 }}>
          <CardContent sx={{ textAlign: "center", py: 5 }}>
            <Box sx={{ mb: 1, color: "text.disabled" }}>
              <InfoIcon sx={{ fontSize: 36 }} />
            </Box>
            <Typography variant="body1" color="text.secondary">
              No issues detected
            </Typography>
            <Typography variant="body2" color="text.disabled" sx={{ mt: 0.5 }}>
              The analysis engine will surface anomalies, root causes, and recommended actions here.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(480px, 1fr))",
            gap: 2,
            mb: 3,
          }}
        >
          {insights.map((insight) => (
            <InsightCard key={insight.id} insight={insight} />
          ))}
        </Box>
      )}
    </Box>
  );
}

const SEVERITY_CONFIG: Record<string, { color: string; bg: string; bgHover: string; icon: typeof ErrorIcon; label: string }> = {
  high: { color: "#c62828", bg: "#fef2f2", bgHover: "#fde8e8", icon: ErrorIcon, label: "High" },
  medium: { color: "#e65100", bg: "#fff8f0", bgHover: "#fff3e6", icon: WarningIcon, label: "Medium" },
  low: { color: "#f9a825", bg: "#fffdf0", bgHover: "#fffbe6", icon: InfoIcon, label: "Low" },
};

function getSeverity(score: number) {
  if (score >= 0.7) return SEVERITY_CONFIG.high;
  if (score >= 0.5) return SEVERITY_CONFIG.medium;
  return SEVERITY_CONFIG.low;
}

const PRIORITY_COLORS: Record<string, string> = {
  high: "#c62828",
  medium: "#e65100",
  low: "#757575",
};

function InsightCard({ insight }: { insight: InsightData }) {
  const [expanded, setExpanded] = useState(false);
  const severity = getSeverity(insight.confidence.score);
  const SeverityIcon = severity.icon;
  const confidencePct = Math.round(insight.confidence.score * 100);

  return (
    <Card
      sx={{
        overflow: "hidden",
        transition: "all 0.2s ease",
        cursor: "pointer",
        "&:hover": {
          boxShadow: "0 4px 20px rgba(0,0,0,0.10)",
          transform: "translateY(-1px)",
        },
      }}
      onClick={() => setExpanded(!expanded)}
    >
      {/* Severity bar */}
      <Box sx={{ height: 4, bgcolor: severity.color }} />

      <CardContent sx={{ p: 2.5, "&:last-child": { pb: 2.5 } }}>
        {/* Header row: icon, headline, expand */}
        <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1.5, mb: 1.5 }}>
          <Box
            sx={{
              width: 36, height: 36, borderRadius: "10px",
              bgcolor: severity.bg,
              display: "flex", alignItems: "center", justifyContent: "center",
              flexShrink: 0,
            }}
          >
            <SeverityIcon sx={{ fontSize: 20, color: severity.color }} />
          </Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography sx={{ fontWeight: 700, fontSize: "0.95rem", lineHeight: 1.3, mb: 0.5, color: "text.primary" }}>
              {insight.headline}
            </Typography>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, flexWrap: "wrap" }}>
              <Typography variant="caption" color="text.secondary">
                {new Date(insight.detected_at + "T00:00:00").toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}
              </Typography>
              <Box sx={{ width: 3, height: 3, borderRadius: "50%", bgcolor: "text.disabled" }} />
              <Typography variant="caption" sx={{ color: severity.color, fontWeight: 600 }}>
                {severity.label} severity
              </Typography>
            </Box>
          </Box>
          <IconButton
            size="small"
            sx={{
              transition: "transform 0.2s",
              transform: expanded ? "rotate(180deg)" : "rotate(0deg)",
            }}
            onClick={(e) => { e.stopPropagation(); setExpanded(!expanded); }}
          >
            <ExpandMoreIcon fontSize="small" />
          </IconButton>
        </Box>

        {/* Signals as inline evidence chips */}
        <Box sx={{ display: "flex", gap: 0.75, flexWrap: "wrap", mb: expanded ? 0 : 0.5 }}>
          {insight.signals.slice(0, 3).map((s, i) => (
            <Chip
              key={i}
              icon={s.change < 0
                ? <TrendingDownIcon sx={{ fontSize: "14px !important" }} />
                : <TrendingUpIcon sx={{ fontSize: "14px !important" }} />
              }
              label={s.description}
              size="small"
              sx={{
                fontSize: "0.72rem", height: 26,
                bgcolor: s.change < 0 ? "#fef2f2" : "#f0fdf4",
                color: s.change < 0 ? "#991b1b" : "#166534",
                border: "1px solid",
                borderColor: s.change < 0 ? "#fecaca" : "#bbf7d0",
                "& .MuiChip-icon": {
                  color: s.change < 0 ? "#dc2626" : "#16a34a",
                },
              }}
            />
          ))}
          {insight.signals.length > 3 && (
            <Chip
              label={`+${insight.signals.length - 3} more`}
              size="small"
              variant="outlined"
              sx={{ fontSize: "0.72rem", height: 26 }}
            />
          )}
        </Box>

        {/* Confidence bar */}
        <Box sx={{ mt: 1.5, display: "flex", alignItems: "center", gap: 1.5 }}>
          <Box sx={{ flex: 1 }}>
            <LinearProgress
              variant="determinate"
              value={confidencePct}
              sx={{
                height: 5, borderRadius: 3,
                bgcolor: "#f0f0f0",
                "& .MuiLinearProgress-bar": {
                  bgcolor: severity.color, borderRadius: 3,
                },
              }}
            />
          </Box>
          <Typography variant="caption" sx={{ fontWeight: 600, color: severity.color, minWidth: 28 }}>
            {confidencePct}%
          </Typography>
          {insight.revenue_impact.value > 0 && (
            <>
              <Box sx={{ width: 1, height: 14, bgcolor: "divider" }} />
              <Typography variant="caption" sx={{ fontWeight: 600, color: "#c62828" }}>
                {insight.revenue_impact.description}
              </Typography>
            </>
          )}
        </Box>

        {/* Expanded detail */}
        <Collapse in={expanded}>
          <Box sx={{ mt: 2, pt: 2, borderTop: "1px solid", borderColor: "divider" }}>
            {/* Description */}
            <Typography variant="body2" sx={{ color: "text.secondary", lineHeight: 1.6, mb: 2 }}>
              {insight.description}
            </Typography>

            {/* All signals if more than 3 */}
            {insight.signals.length > 3 && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 700, color: "text.secondary", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                  All Signals
                </Typography>
                <Box sx={{ mt: 0.75, display: "flex", flexDirection: "column", gap: 0.5 }}>
                  {insight.signals.map((s, i) => (
                    <Box key={i} sx={{ display: "flex", alignItems: "center", gap: 1, py: 0.25 }}>
                      {s.change < 0
                        ? <TrendingDownIcon sx={{ fontSize: 16, color: "#dc2626" }} />
                        : <TrendingUpIcon sx={{ fontSize: 16, color: "#16a34a" }} />
                      }
                      <Typography variant="body2" sx={{ fontSize: "0.82rem" }}>
                        {s.description}
                      </Typography>
                    </Box>
                  ))}
                </Box>
              </Box>
            )}

            {/* Actions */}
            {insight.actions.length > 0 && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 700, color: "text.secondary", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                  Recommended Actions
                </Typography>
                <Box sx={{ mt: 0.75, display: "flex", flexDirection: "column", gap: 0.75 }}>
                  {insight.actions.map((a, i) => (
                    <Box
                      key={i}
                      sx={{
                        display: "flex", alignItems: "flex-start", gap: 1.5,
                        p: 1.25, borderRadius: 1.5,
                        bgcolor: "#f8fafc",
                        border: "1px solid #f0f0f0",
                      }}
                    >
                      <ArrowRightIcon sx={{ fontSize: 18, mt: 0.1, color: PRIORITY_COLORS[a.priority] }} />
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="body2" sx={{ fontSize: "0.82rem", lineHeight: 1.5 }}>
                          {a.description}
                        </Typography>
                      </Box>
                      <Chip
                        label={a.priority}
                        size="small"
                        sx={{
                          fontSize: "0.65rem", height: 20, fontWeight: 700,
                          textTransform: "uppercase", letterSpacing: "0.03em",
                          bgcolor: `${PRIORITY_COLORS[a.priority]}12`,
                          color: PRIORITY_COLORS[a.priority],
                          flexShrink: 0,
                        }}
                      />
                    </Box>
                  ))}
                </Box>
              </Box>
            )}

            {/* Counterfactual / what-if */}
            {insight.counterfactual.description && (
              <Box
                sx={{
                  p: 1.5, borderRadius: 2,
                  bgcolor: "#f0f7ff",
                  border: "1px solid #dbeafe",
                  display: "flex", gap: 1.5, alignItems: "flex-start",
                }}
              >
                <LightbulbIcon sx={{ fontSize: 18, color: "#2563eb", mt: 0.2 }} />
                <Box>
                  <Typography variant="caption" sx={{ fontWeight: 700, color: "#1e40af", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                    What if this is fixed?
                  </Typography>
                  <Typography variant="body2" sx={{ mt: 0.5, fontSize: "0.82rem", color: "#1e3a5f", lineHeight: 1.5 }}>
                    {insight.counterfactual.description}
                    {insight.counterfactual.revenue_impact.description && (
                      <Box component="span" sx={{ fontWeight: 600 }}>
                        {" "}&mdash; {insight.counterfactual.revenue_impact.description}
                      </Box>
                    )}
                  </Typography>
                </Box>
              </Box>
            )}
          </Box>
        </Collapse>
      </CardContent>
    </Card>
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
