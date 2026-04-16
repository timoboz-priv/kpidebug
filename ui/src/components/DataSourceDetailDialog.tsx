import React, { useState, useEffect } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Alert,
  Typography,
  Box,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  Divider,
} from "@mui/material";
import {
  DataSource,
  MetricDescriptor,
  connectDataSource,
  disconnectDataSource,
  discoverMetrics,
} from "../api/dataSources";
import { AvailableSource } from "./DataSourcesSection";

interface DataSourceDetailDialogProps {
  open: boolean;
  onClose: () => void;
  projectId: string;
  source: AvailableSource;
  connectedSource: DataSource | null;
  onChanged: () => void;
}

export default function DataSourceDetailDialog({
  open,
  onClose,
  projectId,
  source,
  connectedSource,
  onChanged,
}: DataSourceDetailDialogProps) {
  const [apiKey, setApiKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [fields, setFields] = useState<MetricDescriptor[]>([]);
  const [fieldsLoading, setFieldsLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);

  const isConnected = connectedSource !== null;

  useEffect(() => {
    if (open && isConnected && connectedSource) {
      setFieldsLoading(true);
      discoverMetrics(projectId, connectedSource.id)
        .then(setFields)
        .catch(() => setFields([]))
        .finally(() => setFieldsLoading(false));
    }
    if (!open) {
      setApiKey("");
      setError(null);
      setFields([]);
    }
  }, [open, isConnected, connectedSource, projectId]);

  const handleConnect = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!apiKey.trim()) return;

    setLoading(true);
    setError(null);
    try {
      await connectDataSource(projectId, {
        name: source.name,
        source_type: source.type,
        credentials: { api_key: apiKey.trim() },
      });
      onChanged();
      setApiKey("");
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to connect");
    } finally {
      setLoading(false);
    }
  };

  const handleDisconnect = async () => {
    if (!connectedSource) return;
    setDisconnecting(true);
    setError(null);
    try {
      await disconnectDataSource(projectId, connectedSource.id);
      onChanged();
      handleClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || "Failed to disconnect");
    } finally {
      setDisconnecting(false);
    }
  };

  const handleClose = () => {
    setApiKey("");
    setError(null);
    onClose();
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
        <Box
          component="img"
          src={source.logo}
          alt={source.name}
          sx={{ width: 28, height: 28 }}
        />
        {source.name}
        {isConnected && (
          <Chip label="Connected" color="success" size="small" sx={{ ml: "auto" }} />
        )}
      </DialogTitle>

      <DialogContent>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          {source.description}
        </Typography>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {isConnected ? (
          <ConnectedView
            fields={fields}
            fieldsLoading={fieldsLoading}
            lastSynced=""
          />
        ) : (
          <ConnectForm
            source={source}
            apiKey={apiKey}
            onApiKeyChange={setApiKey}
            loading={loading}
            onSubmit={handleConnect}
          />
        )}
      </DialogContent>

      <DialogActions>
        {isConnected && (
          <Button
            color="error"
            onClick={handleDisconnect}
            disabled={disconnecting}
            sx={{ mr: "auto" }}
          >
            {disconnecting ? "Disconnecting..." : "Disconnect"}
          </Button>
        )}
        <Button onClick={handleClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

function ConnectedView({
  fields,
  fieldsLoading,
  lastSynced,
}: {
  fields: MetricDescriptor[];
  fieldsLoading: boolean;
  lastSynced: string;
}) {
  return (
    <Box>
      {lastSynced && (
        <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: "block" }}>
          Last synced: {new Date(lastSynced).toLocaleString()}
        </Typography>
      )}

      <Divider sx={{ my: 1.5 }} />

      <Typography variant="subtitle2" sx={{ mb: 1 }}>
        Available Fields
      </Typography>

      {fieldsLoading ? (
        <Box sx={{ display: "flex", justifyContent: "center", py: 3 }}>
          <CircularProgress size={24} />
        </Box>
      ) : (
        <TableContainer sx={{ maxHeight: 340 }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Field</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Dimensions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {fields.map((field) => (
                <TableRow key={field.key}>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: "monospace" }}>
                      {field.key}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {field.description}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap" }}>
                      {field.dimensions.map((d) => (
                        <Chip
                          key={d.name}
                          label={d.name}
                          size="small"
                          variant="outlined"
                          color={d.type === "temporal" ? "primary" : "default"}
                        />
                      ))}
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
}

function ConnectForm({
  source,
  apiKey,
  onApiKeyChange,
  loading,
  onSubmit,
}: {
  source: AvailableSource;
  apiKey: string;
  onApiKeyChange: (value: string) => void;
  loading: boolean;
  onSubmit: (e: React.FormEvent) => void;
}) {
  if (source.authMethod === "api_key") {
    return (
      <form onSubmit={onSubmit}>
        <Typography variant="subtitle2" sx={{ mb: 1 }}>
          Connect with API Key
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          {source.authHint}
        </Typography>
        <TextField
          autoFocus
          fullWidth
          label="API Key"
          type="password"
          value={apiKey}
          onChange={(e) => onApiKeyChange(e.target.value)}
          placeholder={source.authPlaceholder}
          margin="normal"
          required
        />
        <Button
          type="submit"
          variant="contained"
          fullWidth
          disabled={loading || !apiKey.trim()}
          sx={{ mt: 1 }}
        >
          {loading ? "Connecting..." : "Connect"}
        </Button>
      </form>
    );
  }

  if (source.authMethod === "oauth") {
    return (
      <Box>
        <Typography variant="subtitle2" sx={{ mb: 1 }}>
          Connect with OAuth
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          {source.authHint}
        </Typography>
        <Button variant="contained" fullWidth disabled>
          Sign in with {source.name} (Coming soon)
        </Button>
      </Box>
    );
  }

  return null;
}
