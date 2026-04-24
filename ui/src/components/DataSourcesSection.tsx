import React, { useEffect, useState, useCallback } from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  CardActionArea,
  Grid,
  Chip,
  Alert,
  Skeleton,
} from "@mui/material";
import {
  CheckCircle as CheckCircleIcon,
  CloudQueue as CloudIcon,
} from "@mui/icons-material";
import { DataSource, listDataSources } from "../api/dataSources";
import DataSourceDetailDialog from "./DataSourceDetailDialog";

export interface AvailableSource {
  type: string;
  name: string;
  description: string;
  logo: string;
  authMethod: "api_key" | "oauth" | "service_account";
  authHint: string;
  authPlaceholder?: string;
}

const AVAILABLE_SOURCES: AvailableSource[] = [
  {
    type: "stripe",
    name: "Stripe",
    description:
      "Payment processing platform. Pull charges, subscriptions, invoices, customers, and refund data with breakdowns by currency, status, and time.",
    logo: "https://cdn.simpleicons.org/stripe/635BFF",
    authMethod: "api_key",
    authHint:
      "Enter a Stripe restricted API key with read access. You can create one in your Stripe Dashboard under Developers > API keys.",
    authPlaceholder: "sk_live_... or rk_live_...",
  },
  {
    type: "google_analytics",
    name: "Google Analytics",
    description:
      "Web and app analytics. Track page views, sessions, users, conversions, and engagement metrics across dimensions like geography, device, and traffic source.",
    logo: "https://cdn.simpleicons.org/googleanalytics/E37400",
    authMethod: "service_account",
    authHint:
      "Enter your GA4 Property ID and paste the service account JSON key. The service account must have Viewer access on the GA4 property.",
  },
  {
    type: "datadog",
    name: "Datadog",
    description:
      "Infrastructure and application monitoring. Pull metrics on latency, error rates, throughput, and resource utilization across services and environments.",
    logo: "https://cdn.simpleicons.org/datadog/632CA6",
    authMethod: "api_key",
    authHint: "Enter your Datadog API key and application key.",
    authPlaceholder: "API key",
  },
  {
    type: "google_cloud",
    name: "Google Cloud",
    description:
      "Cloud platform metrics and billing. Monitor spend, resource usage, and operational metrics across your GCP projects and services.",
    logo: "https://cdn.simpleicons.org/googlecloud/4285F4",
    authMethod: "oauth",
    authHint: "Sign in with your Google account to grant read access to your Cloud projects.",
  },
  {
    type: "tableau",
    name: "Tableau",
    description:
      "Business intelligence and data visualization. Connect to your Tableau dashboards and pull underlying metric data for cross-platform analysis.",
    logo: "https://cdn.simpleicons.org/tableau/E97627",
    authMethod: "api_key",
    authHint: "Enter your Tableau Server personal access token.",
    authPlaceholder: "Personal access token",
  },
];

interface DataSourcesSectionProps {
  projectId: string;
}

export default function DataSourcesSection({ projectId }: DataSourcesSectionProps) {
  const [connectedSources, setConnectedSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedSource, setSelectedSource] = useState<AvailableSource | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);

  const fetchSources = useCallback(async () => {
    setLoading(true);
    try {
      const sources = await listDataSources(projectId);
      setConnectedSources(sources);
      setError(null);
    } catch (err: any) {
      setError("Failed to load data sources");
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    fetchSources();
  }, [fetchSources]);

  const connectedByType = new Map(connectedSources.map((s) => [s.type, s]));

  const handleTileClick = (source: AvailableSource) => {
    setSelectedSource(source);
    setDialogOpen(true);
  };

  const handleDialogClose = () => {
    setDialogOpen(false);
    setSelectedSource(null);
  };

  return (
    <>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Connect your data sources to start pulling metrics. Click a source to see details or connect.
      </Typography>

      {loading ? (
        <Grid container spacing={2}>
          {[1, 2, 3].map((i) => (
            <Grid size={{ xs: 12, sm: 6, md: 4 }} key={i}>
              <Skeleton variant="rounded" height={180} />
            </Grid>
          ))}
        </Grid>
      ) : (
        <Grid container spacing={2}>
          {AVAILABLE_SOURCES.map((source) => {
            const connected = connectedByType.get(source.type);
            return (
              <Grid size={{ xs: 12, sm: 6, md: 4 }} key={source.type}>
                <Card
                  sx={{
                    height: "100%",
                    position: "relative",
                    border: connected ? "2px solid" : "1px solid",
                    borderColor: connected ? "success.main" : "divider",
                    transition: "border-color 0.2s, box-shadow 0.2s",
                  }}
                >
                  <CardActionArea
                    onClick={() => handleTileClick(source)}
                    sx={{
                      height: "100%",
                      display: "flex",
                      flexDirection: "column",
                      alignItems: "stretch",
                    }}
                  >
                    <CardContent sx={{ flex: 1, display: "flex", flexDirection: "column" }}>
                      <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 1.5 }}>
                        <Box
                          component="img"
                          src={source.logo}
                          alt={source.name}
                          sx={{ width: 32, height: 32 }}
                        />
                        <Typography variant="h6" sx={{ flex: 1 }}>
                          {source.name}
                        </Typography>
                        {connected && <CheckCircleIcon color="success" fontSize="small" />}
                      </Box>
                      <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{
                          flex: 1,
                          display: "-webkit-box",
                          WebkitLineClamp: 3,
                          WebkitBoxOrient: "vertical",
                          overflow: "hidden",
                        }}
                      >
                        {source.description}
                      </Typography>
                      <Box sx={{ mt: 1.5 }}>
                        {connected ? (
                          <Chip
                            label="Connected"
                            color="success"
                            size="small"
                            variant="outlined"
                          />
                        ) : (
                          <Chip
                            icon={<CloudIcon />}
                            label="Not connected"
                            size="small"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    </CardContent>
                  </CardActionArea>
                </Card>
              </Grid>
            );
          })}
        </Grid>
      )}

      {selectedSource && (
        <DataSourceDetailDialog
          open={dialogOpen}
          onClose={handleDialogClose}
          projectId={projectId}
          source={selectedSource}
          connectedSource={connectedByType.get(selectedSource.type) || null}
          onChanged={fetchSources}
        />
      )}
    </>
  );
}
