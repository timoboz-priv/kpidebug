import React, { useEffect, useState, useCallback, useRef } from "react";
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
  Chip,
  Popover,
  ToggleButton,
  ToggleButtonGroup,
} from "@mui/material";
import {
  PlayArrow as RunIcon,
  Add as AddIcon,
  ArrowUpward as AscIcon,
  ArrowDownward as DescIcon,
  Sync as SyncIcon,
  Close as CloseIcon,
} from "@mui/icons-material";
import { useProject } from "../contexts/ProjectContext";
import {
  DataSource,
  TableColumn,
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

// --- Filter config model (richer than the API TableFilter) ---

interface FilterConfig {
  id: string;
  column: string;
  columnType: string;
  operator: string;
  value: string;
  dateMode?: "relative" | "absolute";
  relativeAmount?: number;
  relativeUnit?: "days" | "weeks" | "months";
  absoluteStart?: string;
  absoluteEnd?: string;
}

let _filterId = 0;
function nextFilterId(): string {
  return `f${++_filterId}`;
}

function filterToApi(f: FilterConfig): TableFilter[] {
  if (f.columnType === "datetime") {
    const filters: TableFilter[] = [];
    if (f.dateMode === "relative" && f.relativeAmount && f.relativeUnit) {
      const d = new Date();
      if (f.relativeUnit === "days") d.setDate(d.getDate() - f.relativeAmount);
      else if (f.relativeUnit === "weeks") d.setDate(d.getDate() - f.relativeAmount * 7);
      else if (f.relativeUnit === "months") d.setMonth(d.getMonth() - f.relativeAmount);
      filters.push({ column: f.column, operator: "gte", value: d.toISOString() });
    } else if (f.dateMode === "absolute") {
      if (f.absoluteStart) filters.push({ column: f.column, operator: "gte", value: new Date(f.absoluteStart).toISOString() });
      if (f.absoluteEnd) filters.push({ column: f.column, operator: "lte", value: new Date(f.absoluteEnd).toISOString() });
    }
    return filters;
  }
  if (!f.value) return [];
  return [{ column: f.column, operator: f.operator, value: f.value }];
}

function filterLabel(f: FilterConfig, columns: TableColumn[]): string {
  const col = columns.find((c) => c.key === f.column);
  const colName = col?.name || f.column;

  if (f.columnType === "datetime") {
    if (f.dateMode === "relative" && f.relativeAmount && f.relativeUnit) {
      return `${colName}: last ${f.relativeAmount} ${f.relativeUnit}`;
    }
    if (f.dateMode === "absolute") {
      const parts: string[] = [];
      if (f.absoluteStart) parts.push(`from ${f.absoluteStart}`);
      if (f.absoluteEnd) parts.push(`to ${f.absoluteEnd}`);
      return `${colName}: ${parts.join(" ")}`;
    }
    return colName;
  }

  const opLabels: Record<string, string> = {
    eq: "=", neq: "\u2260", gt: ">", gte: "\u2265", lt: "<", lte: "\u2264", contains: "\u2248",
  };
  return `${colName} ${opLabels[f.operator] || f.operator} ${f.value}`;
}

function getOperatorsForType(type: string): { value: string; label: string }[] {
  if (type === "number" || type === "currency") {
    return [
      { value: "eq", label: "equals" },
      { value: "neq", label: "not equals" },
      { value: "gt", label: "greater than" },
      { value: "gte", label: "greater or equal" },
      { value: "lt", label: "less than" },
      { value: "lte", label: "less or equal" },
    ];
  }
  if (type === "boolean") {
    return [
      { value: "eq", label: "equals" },
    ];
  }
  return [
    { value: "eq", label: "equals" },
    { value: "neq", label: "not equals" },
    { value: "contains", label: "contains" },
  ];
}

function makeDefaultDateFilter(columns: TableColumn[]): FilterConfig | null {
  const dateCol = columns.find((c) => c.type === "datetime");
  if (!dateCol) return null;
  return {
    id: nextFilterId(),
    column: dateCol.key,
    columnType: "datetime",
    operator: "gte",
    value: "",
    dateMode: "relative",
    relativeAmount: 30,
    relativeUnit: "days",
  };
}

// --- Main page ---

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

  const [filters, setFilters] = useState<FilterConfig[]>([]);
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  const [result, setResult] = useState<TableQueryResponse | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);

  const [popoverAnchor, setPopoverAnchor] = useState<HTMLElement | null>(null);
  const [editingFilter, setEditingFilter] = useState<FilterConfig | null>(null);
  const [editorStep, setEditorStep] = useState<"column" | "config">("column");

  const shouldAutoQuery = useRef(false);

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

  const handleRun = useCallback(async () => {
    if (!currentProject || !selectedTable || !selectedSourceId) return;
    setQueryLoading(true);
    setQueryError(null);
    try {
      const apiFilters = filters.flatMap(filterToApi);
      const res = await queryTable(currentProject.id, {
        source_id: selectedSourceId,
        table: selectedTable.key,
        filters: apiFilters.length > 0 ? apiFilters : undefined,
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

  useEffect(() => {
    if (shouldAutoQuery.current) {
      shouldAutoQuery.current = false;
      handleRun();
    }
  }, [handleRun]);

  const handleTableSelect = (sourceId: string, table: TableDescriptor) => {
    setSelectedSourceId(sourceId);
    setSelectedTable(table);
    setResult(null);
    setQueryError(null);
    setSortBy(null);
    setSortOrder("desc");
    setPage(0);

    const dateFilter = makeDefaultDateFilter(table.columns);
    setFilters(dateFilter ? [dateFilter] : []);
    shouldAutoQuery.current = true;
  };

  const handleSync = async () => {
    if (!currentProject || !selectedTable || !selectedSourceId) return;
    setSyncing(true);
    setQueryError(null);
    try {
      await syncTable(currentProject.id, selectedSourceId, selectedTable.key);
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

  // --- Filter popover ---

  const openAddFilter = (event: React.MouseEvent<HTMLElement>) => {
    setEditingFilter(null);
    setEditorStep("column");
    setPopoverAnchor(event.currentTarget);
  };

  const openEditFilter = (event: React.MouseEvent<HTMLElement>, filter: FilterConfig) => {
    setEditingFilter({ ...filter });
    setEditorStep("config");
    setPopoverAnchor(event.currentTarget);
  };

  const closePopover = () => {
    setPopoverAnchor(null);
    setEditingFilter(null);
  };

  const handleColumnSelect = (col: TableColumn) => {
    const newFilter: FilterConfig = {
      id: nextFilterId(),
      column: col.key,
      columnType: col.type,
      operator: col.type === "datetime" ? "gte" : "eq",
      value: "",
      ...(col.type === "datetime" ? { dateMode: "relative" as const, relativeAmount: 30, relativeUnit: "days" as const } : {}),
      ...(col.type === "boolean" ? { value: "true" } : {}),
    };
    setEditingFilter(newFilter);
    setEditorStep("config");
  };

  const handleSaveFilter = () => {
    if (!editingFilter) return;
    const existing = filters.findIndex((f) => f.id === editingFilter.id);
    if (existing >= 0) {
      const updated = [...filters];
      updated[existing] = editingFilter;
      setFilters(updated);
    } else {
      setFilters([...filters, editingFilter]);
    }
    closePopover();
  };

  const handleRemoveFilter = (id: string) => {
    setFilters(filters.filter((f) => f.id !== id));
  };

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
                    lineHeight: "32px", bgcolor: "background.paper", fontWeight: 600,
                    fontSize: "0.75rem", textTransform: "uppercase",
                    letterSpacing: "0.05em", color: "text.secondary",
                  }}
                >
                  {source.name}
                </ListSubheader>
                {tables.map((table) => (
                  <ListItemButton
                    key={table.key}
                    selected={selectedTable?.key === table.key && selectedSourceId === source.id}
                    onClick={() => handleTableSelect(source.id, table)}
                    sx={{
                      py: 0.5, pl: 3, borderRadius: 0,
                      "&.Mui-selected": {
                        bgcolor: "primary.main", color: "white",
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
      <Box sx={{ flex: 1, overflow: "hidden", p: 3, display: "flex", flexDirection: "column", minWidth: 0 }}>
        {error && <Alert severity="error" sx={{ mb: 2, flexShrink: 0 }}>{error}</Alert>}

        {!selectedTable ? (
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1, color: "text.secondary" }}>
            <Typography variant="body1">Select a table from the left to explore</Typography>
          </Box>
        ) : (
          <>
            {/* Table metadata */}
            <Card sx={{ mb: 2, flexShrink: 0 }}>
              <CardContent sx={{ py: 1.5, "&:last-child": { pb: 1.5 } }}>
                <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <Box>
                    <Typography variant="h6">{selectedTable.name}</Typography>
                    <Typography variant="body2" color="text.secondary">{selectedTable.description}</Typography>
                  </Box>
                  <Tooltip title="Sync data from source">
                    <IconButton onClick={handleSync} disabled={syncing}>
                      <SyncIcon sx={{
                        animation: syncing ? "spin 1s linear infinite" : "none",
                        "@keyframes spin": { "100%": { transform: "rotate(360deg)" } },
                      }} />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap", mt: 1.5 }}>
                  {selectedTable.columns.map((col) => (
                    <ColumnChip key={col.key} column={col} />
                  ))}
                </Box>
              </CardContent>
            </Card>

            {/* Filter bar */}
            <Card sx={{ mb: 2, flexShrink: 0 }}>
              <CardContent sx={{ py: 1, "&:last-child": { pb: 1 } }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1, flexWrap: "wrap" }}>
                  {filters.map((f) => (
                    <Chip
                      key={f.id}
                      label={filterLabel(f, selectedTable.columns)}
                      onClick={(e) => openEditFilter(e, f)}
                      onDelete={() => handleRemoveFilter(f.id)}
                      variant="outlined"
                      size="small"
                      sx={{ cursor: "pointer" }}
                    />
                  ))}
                  <IconButton size="small" onClick={openAddFilter}>
                    <AddIcon fontSize="small" />
                  </IconButton>
                  <Box sx={{ flex: 1 }} />
                  <Button
                    variant="contained"
                    size="small"
                    startIcon={<RunIcon />}
                    onClick={handleRun}
                    disabled={queryLoading}
                  >
                    {queryLoading ? "Loading..." : "Query"}
                  </Button>
                </Box>
              </CardContent>
            </Card>

            {/* Filter editor popover */}
            <Popover
              open={Boolean(popoverAnchor)}
              anchorEl={popoverAnchor}
              onClose={closePopover}
              anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
              transformOrigin={{ vertical: "top", horizontal: "left" }}
            >
              <Box sx={{ p: 2, width: 320 }}>
                {editorStep === "column" ? (
                  <ColumnPicker columns={selectedTable.columns} onSelect={handleColumnSelect} />
                ) : editingFilter ? (
                  <FilterEditor
                    filter={editingFilter}
                    onChange={setEditingFilter}
                    onSave={handleSaveFilter}
                    onClose={closePopover}
                    columns={selectedTable.columns}
                  />
                ) : null}
              </Box>
            </Popover>

            {queryError && <Alert severity="error" sx={{ mb: 2 }}>{queryError}</Alert>}

            {queryLoading && (
              <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                <CircularProgress />
              </Box>
            )}

            {result && !queryLoading && (
              <Card sx={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", minHeight: 0 }}>
                <TableContainer sx={{ flex: 1, overflow: "auto" }}>
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
                                sortOrder === "asc" ? <AscIcon sx={{ fontSize: 16 }} /> : <DescIcon sx={{ fontSize: 16 }} />
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
                            <Typography variant="body2" color="text.secondary">No rows found</Typography>
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

// --- Sub-components ---

function ColumnPicker({
  columns,
  onSelect,
}: {
  columns: TableColumn[];
  onSelect: (col: TableColumn) => void;
}) {
  return (
    <Box>
      <Typography variant="subtitle2" sx={{ mb: 1 }}>Select column</Typography>
      <List dense disablePadding>
        {columns.map((col) => (
          <ListItemButton key={col.key} onClick={() => onSelect(col)} sx={{ borderRadius: 1 }}>
            <ListItemText
              primary={col.name}
              secondary={col.type}
              slotProps={{ secondary: { sx: { fontSize: "0.7rem", textTransform: "uppercase" } } }}
            />
          </ListItemButton>
        ))}
      </List>
    </Box>
  );
}

function FilterEditor({
  filter,
  onChange,
  onSave,
  onClose,
  columns,
}: {
  filter: FilterConfig;
  onChange: (f: FilterConfig) => void;
  onSave: () => void;
  onClose: () => void;
  columns: TableColumn[];
}) {
  const col = columns.find((c) => c.key === filter.column);
  const colName = col?.name || filter.column;

  if (filter.columnType === "datetime") {
    return (
      <DateFilterEditor filter={filter} onChange={onChange} onSave={onSave} onClose={onClose} colName={colName} />
    );
  }

  const operators = getOperatorsForType(filter.columnType);

  return (
    <Box>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
        <Typography variant="subtitle2">{colName}</Typography>
        <IconButton size="small" onClick={onClose}><CloseIcon fontSize="small" /></IconButton>
      </Box>

      {filter.columnType === "boolean" ? (
        <FormControl fullWidth size="small" sx={{ mb: 1.5 }}>
          <InputLabel>Value</InputLabel>
          <Select value={filter.value} label="Value" onChange={(e) => onChange({ ...filter, value: e.target.value })}>
            <MenuItem value="true">True</MenuItem>
            <MenuItem value="false">False</MenuItem>
          </Select>
        </FormControl>
      ) : (
        <>
          <FormControl fullWidth size="small" sx={{ mb: 1.5 }}>
            <InputLabel>Operator</InputLabel>
            <Select value={filter.operator} label="Operator" onChange={(e) => onChange({ ...filter, operator: e.target.value })}>
              {operators.map((op) => (
                <MenuItem key={op.value} value={op.value}>{op.label}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth size="small" label="Value" value={filter.value}
            type={filter.columnType === "number" || filter.columnType === "currency" ? "number" : "text"}
            onChange={(e) => onChange({ ...filter, value: e.target.value })}
            sx={{ mb: 1.5 }}
          />
        </>
      )}

      <Button variant="contained" fullWidth size="small" onClick={onSave}>Apply</Button>
    </Box>
  );
}

function DateFilterEditor({
  filter,
  onChange,
  onSave,
  onClose,
  colName,
}: {
  filter: FilterConfig;
  onChange: (f: FilterConfig) => void;
  onSave: () => void;
  onClose: () => void;
  colName: string;
}) {
  const mode = filter.dateMode || "relative";

  return (
    <Box>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1.5 }}>
        <Typography variant="subtitle2">{colName}</Typography>
        <IconButton size="small" onClick={onClose}><CloseIcon fontSize="small" /></IconButton>
      </Box>

      <ToggleButtonGroup
        value={mode}
        exclusive
        onChange={(_, v) => { if (v) onChange({ ...filter, dateMode: v }); }}
        size="small"
        fullWidth
        sx={{ mb: 1.5 }}
      >
        <ToggleButton value="relative">Relative</ToggleButton>
        <ToggleButton value="absolute">Absolute</ToggleButton>
      </ToggleButtonGroup>

      {mode === "relative" ? (
        <Box sx={{ display: "flex", gap: 1, mb: 1.5 }}>
          <TextField
            size="small" label="Last" type="number" value={filter.relativeAmount || ""}
            onChange={(e) => onChange({ ...filter, relativeAmount: parseInt(e.target.value) || 0 })}
            sx={{ width: 80 }}
          />
          <FormControl size="small" sx={{ flex: 1 }}>
            <InputLabel>Unit</InputLabel>
            <Select
              value={filter.relativeUnit || "days"}
              label="Unit"
              onChange={(e) => onChange({ ...filter, relativeUnit: e.target.value as "days" | "weeks" | "months" })}
            >
              <MenuItem value="days">days</MenuItem>
              <MenuItem value="weeks">weeks</MenuItem>
              <MenuItem value="months">months</MenuItem>
            </Select>
          </FormControl>
        </Box>
      ) : (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5, mb: 1.5 }}>
          <TextField
            size="small" label="From" type="date"
            value={filter.absoluteStart || ""}
            onChange={(e) => onChange({ ...filter, absoluteStart: e.target.value })}
            slotProps={{ inputLabel: { shrink: true } }}
          />
          <TextField
            size="small" label="To" type="date"
            value={filter.absoluteEnd || ""}
            onChange={(e) => onChange({ ...filter, absoluteEnd: e.target.value })}
            slotProps={{ inputLabel: { shrink: true } }}
          />
        </Box>
      )}

      <Button variant="contained" fullWidth size="small" onClick={onSave}>Apply</Button>
    </Box>
  );
}

function CellValue({ value, type }: { value: unknown; type: string }) {
  if (value === null || value === undefined || value === "") {
    return <Typography variant="body2" color="text.disabled">&mdash;</Typography>;
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

  if (type === "boolean") {
    return <Typography variant="body2">{value ? "Yes" : "No"}</Typography>;
  }

  return <Typography variant="body2">{String(value)}</Typography>;
}
