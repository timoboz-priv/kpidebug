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
  TextField,
  IconButton,
  TablePagination,
  Tooltip,
} from "@mui/material";
import {
  PlayArrow as RunIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  ArrowUpward as AscIcon,
  ArrowDownward as DescIcon,
  Sync as SyncIcon,
} from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import {
  DataSource,
  TableDescriptor,
  TableFilter,
  TableQueryResponse,
  listDataSources,
  discoverTables,
  queryTable,
  syncTable,
} from "../api/dataSources";
import ColumnChip from "../components/ColumnChip";

const PANEL_WIDTH = 260;

const FILTER_OPERATORS = [
  { value: "eq", label: "=" },
  { value: "neq", label: "!=" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "contains", label: "contains" },
];

interface SourceWithTables {
  source: DataSource;
  tables: TableDescriptor[];
}

export default function DataTablesPage() {
  const { currentProject } = useProject();
  const [sources, setSources] = useState<SourceWithTables[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<TableDescriptor | null>(null);

  const [filters, setFilters] = useState<TableFilter[]>([]);
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  const [result, setResult] = useState<TableQueryResponse | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);

  const fetchSources = useCallback(async () => {
    if (!currentProject) return;
    setLoading(true);
    try {
      const srcList = await listDataSources(currentProject.id);
      const withTables: SourceWithTables[] = [];
      for (const source of srcList) {
        try {
          const tables = await discoverTables(currentProject.id, source.id);
          withTables.push({ source, tables });
        } catch {
          withTables.push({ source, tables: [] });
        }
      }
      setSources(withTables);
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

  const handleTableSelect = (sourceId: string, table: TableDescriptor) => {
    setSelectedSourceId(sourceId);
    setSelectedTable(table);
    setResult(null);
    setQueryError(null);
    setFilters([]);
    setSortBy(null);
    setSortOrder("desc");
    setPage(0);
  };

  const handleRun = useCallback(async () => {
    if (!currentProject || !selectedTable || !selectedSourceId) return;
    setQueryLoading(true);
    setQueryError(null);
    try {
      const validFilters = filters.filter((f) => f.column && f.value);
      const res = await queryTable(currentProject.id, {
        source_id: selectedSourceId,
        table: selectedTable.key,
        filters: validFilters.length > 0 ? validFilters : undefined,
        sort_by: sortBy,
        sort_order: sortOrder,
        limit: rowsPerPage,
        offset: page * rowsPerPage,
      });
      setResult(res);
    } catch (err: any) {
      setQueryError(err.response?.data?.detail || "Query failed");
    } finally {
      setQueryLoading(false);
    }
  }, [currentProject, selectedTable, selectedSourceId, filters, sortBy, sortOrder, page, rowsPerPage]);

  const handleSync = async () => {
    if (!currentProject || !selectedTable || !selectedSourceId) return;
    setSyncing(true);
    setQueryError(null);
    try {
      await syncTable(currentProject.id, selectedSourceId, selectedTable.key);
      // Re-run query to show fresh data
      await handleRun();
    } catch (err: any) {
      setQueryError(err.response?.data?.detail || "Sync failed");
    } finally {
      setSyncing(false);
    }
  };

  const handleSort = (columnKey: string) => {
    if (sortBy === columnKey) {
      setSortOrder(sortOrder === "asc" ? "desc" : "asc");
    } else {
      setSortBy(columnKey);
      setSortOrder("desc");
    }
  };

  const handleAddFilter = () => {
    setFilters([...filters, { column: "", operator: "eq", value: "" }]);
  };

  const handleUpdateFilter = (index: number, filter: TableFilter) => {
    const updated = [...filters];
    updated[index] = filter;
    setFilters(updated);
  };

  const handleRemoveFilter = (index: number) => {
    setFilters(filters.filter((_, i) => i !== index));
  };

  if (!currentProject) return null;

  return (
    <Box sx={{ display: "flex", height: "calc(100vh - 48px)" }}>
      {/* Left panel: table tree */}
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
          <Typography variant="h6">Data</Typography>
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
            {sources.map(({ source, tables }) => (
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
                {tables.map((table) => (
                  <ListItemButton
                    key={table.key}
                    selected={
                      selectedTable?.key === table.key && selectedSourceId === source.id
                    }
                    onClick={() => handleTableSelect(source.id, table)}
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
                    <ListItemText primary={table.name} />
                  </ListItemButton>
                ))}
              </React.Fragment>
            ))}
          </List>
        )}
      </Box>

      {/* Right area */}
      <Box sx={{ flex: 1, overflowY: "auto", p: 3, display: "flex", flexDirection: "column" }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {!selectedTable ? (
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              flex: 1,
              color: "text.secondary",
            }}
          >
            <Typography variant="body1">Select a table from the left to explore</Typography>
          </Box>
        ) : (
          <>
            {/* Table metadata */}
            <Card sx={{ mb: 2 }}>
              <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
                <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <Box>
                    <Typography variant="h6">{selectedTable.name}</Typography>
                    <Typography variant="body2" color="text.secondary">
                      {selectedTable.description}
                    </Typography>
                  </Box>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <Tooltip title="Sync data from source">
                      <IconButton onClick={handleSync} disabled={syncing}>
                        <SyncIcon
                          sx={{
                            animation: syncing ? "spin 1s linear infinite" : "none",
                            "@keyframes spin": { "100%": { transform: "rotate(360deg)" } },
                          }}
                        />
                      </IconButton>
                    </Tooltip>
                  </Box>
                </Box>
                <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap", mt: 1.5 }}>
                  {selectedTable.columns.map((col) => (
                    <ColumnChip key={col.key} column={col} />
                  ))}
                </Box>
              </CardContent>
            </Card>

            {/* Controls */}
            <Card sx={{ mb: 2 }}>
              <CardContent>
                <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", alignItems: "center" }}>
                  <Button
                    variant="contained"
                    startIcon={<RunIcon />}
                    onClick={handleRun}
                    disabled={queryLoading}
                  >
                    {queryLoading ? "Loading..." : "Query"}
                  </Button>
                </Box>

                {/* Filters */}
                <Box sx={{ mt: 2 }}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
                    <Typography variant="subtitle2">Filters</Typography>
                    <IconButton size="small" onClick={handleAddFilter}>
                      <AddIcon fontSize="small" />
                    </IconButton>
                  </Box>
                  {filters.map((filter, i) => (
                    <Box key={i} sx={{ display: "flex", gap: 1, mb: 1, alignItems: "center" }}>
                      <FormControl size="small" sx={{ minWidth: 140 }}>
                        <InputLabel>Column</InputLabel>
                        <Select
                          value={filter.column}
                          label="Column"
                          onChange={(e) =>
                            handleUpdateFilter(i, { ...filter, column: e.target.value })
                          }
                        >
                          {selectedTable.columns.map((col) => (
                            <MenuItem key={col.key} value={col.key}>
                              {col.name}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                      <FormControl size="small" sx={{ minWidth: 100 }}>
                        <InputLabel>Op</InputLabel>
                        <Select
                          value={filter.operator}
                          label="Op"
                          onChange={(e) =>
                            handleUpdateFilter(i, { ...filter, operator: e.target.value })
                          }
                        >
                          {FILTER_OPERATORS.map((op) => (
                            <MenuItem key={op.value} value={op.value}>
                              {op.label}
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
                        sx={{ width: 180 }}
                      />
                      <IconButton size="small" onClick={() => handleRemoveFilter(i)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
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

            {result && !queryLoading && (
              <Card sx={{ flex: 1, display: "flex", flexDirection: "column" }}>
                <TableContainer sx={{ flex: 1 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        {result.columns.map((col) => (
                          <TableCell
                            key={col.key}
                            sx={{ cursor: "pointer", userSelect: "none", fontWeight: 600 }}
                            onClick={() => handleSort(col.key)}
                          >
                            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                              {col.name}
                              {sortBy === col.key && (
                                sortOrder === "asc" ? (
                                  <AscIcon sx={{ fontSize: 16 }} />
                                ) : (
                                  <DescIcon sx={{ fontSize: 16 }} />
                                )
                              )}
                            </Box>
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {result.rows.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={result.columns.length} sx={{ textAlign: "center", py: 4 }}>
                            <Typography variant="body2" color="text.secondary">
                              No rows found
                            </Typography>
                          </TableCell>
                        </TableRow>
                      ) : (
                        result.rows.map((row, i) => (
                          <TableRow key={i} hover>
                            {result.columns.map((col) => (
                              <TableCell key={col.key}>
                                <CellValue value={row[col.key]} type={col.type} />
                              </TableCell>
                            ))}
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
                <TablePagination
                  component="div"
                  count={result.total_count}
                  page={page}
                  onPageChange={(_, p) => setPage(p)}
                  rowsPerPage={rowsPerPage}
                  onRowsPerPageChange={(e) => {
                    setRowsPerPage(parseInt(e.target.value, 10));
                    setPage(0);
                  }}
                  rowsPerPageOptions={[10, 25, 50, 100]}
                />
              </Card>
            )}
          </>
        )}
      </Box>
    </Box>
  );
}

function CellValue({ value, type }: { value: unknown; type: string }) {
  if (value === null || value === undefined || value === "") {
    return <Typography variant="body2" color="text.disabled">—</Typography>;
  }

  if (type === "datetime" && typeof value === "string") {
    try {
      return (
        <Typography variant="body2" sx={{ fontFamily: "monospace", fontSize: "0.8rem" }}>
          {new Date(value).toLocaleString()}
        </Typography>
      );
    } catch {
      return <Typography variant="body2">{String(value)}</Typography>;
    }
  }

  if (type === "number" || type === "currency") {
    return (
      <Typography variant="body2" sx={{ fontFamily: "monospace" }}>
        {Number(value).toLocaleString()}
      </Typography>
    );
  }

  return <Typography variant="body2">{String(value)}</Typography>;
}
