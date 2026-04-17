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
import { Sync as SyncIcon } from "@mui/icons-material";
import {
  DataSource,
  TableDescriptor,
  connectDataSource,
  disconnectDataSource,
  discoverTables,
  syncSource,
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
  const [tables, setTables] = useState<TableDescriptor[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [syncing, setSyncing] = useState(false);

  const isConnected = connectedSource !== null;

  useEffect(() => {
    if (open && isConnected && connectedSource) {
      setTablesLoading(true);
      discoverTables(projectId, connectedSource.id)
        .then(setTables)
        .catch(() => setTables([]))
        .finally(() => setTablesLoading(false));
    }
    if (!open) {
      setApiKey("");
      setError(null);
      setTables([]);
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

  const handleSync = async () => {
    if (!connectedSource) return;
    setSyncing(true);
    setError(null);
    try {
      await syncSource(projectId, connectedSource.id);
    } catch (err: any) {
      setError(err.response?.data?.detail || "Sync failed");
    } finally {
      setSyncing(false);
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
            tables={tables}
            tablesLoading={tablesLoading}
            syncing={syncing}
            onSync={handleSync}
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
  tables,
  tablesLoading,
  syncing,
  onSync,
}: {
  tables: TableDescriptor[];
  tablesLoading: boolean;
  syncing: boolean;
  onSync: () => void;
}) {
  return (
    <Box>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1 }}>
        <Typography variant="subtitle2">
          Available Tables
        </Typography>
        <Button
          size="small"
          startIcon={
            <SyncIcon
              sx={{
                animation: syncing ? "spin 1s linear infinite" : "none",
                "@keyframes spin": { "100%": { transform: "rotate(360deg)" } },
              }}
            />
          }
          onClick={onSync}
          disabled={syncing}
        >
          {syncing ? "Syncing..." : "Sync All"}
        </Button>
      </Box>

      <Divider sx={{ mb: 1.5 }} />

      {tablesLoading ? (
        <Box sx={{ display: "flex", justifyContent: "center", py: 3 }}>
          <CircularProgress size={24} />
        </Box>
      ) : (
        <TableContainer sx={{ maxHeight: 340 }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Table</TableCell>
                <TableCell>Description</TableCell>
                <TableCell align="right">Columns</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {tables.map((table) => (
                <TableRow key={table.key}>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: "monospace" }}>
                      {table.key}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {table.description}
                    </Typography>
                  </TableCell>
                  <TableCell align="right">
                    <Typography variant="body2" color="text.secondary">
                      {table.columns.length}
                    </Typography>
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
