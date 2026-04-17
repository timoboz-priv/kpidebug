import React from "react";
import {
  Box,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
} from "@mui/material";

export interface TimeRange {
  mode: "relative" | "absolute";
  relativeAmount: number;
  relativeUnit: "days" | "weeks" | "months";
  absoluteStart: string;
  absoluteEnd: string;
}

export const DEFAULT_TIME_RANGE: TimeRange = {
  mode: "relative",
  relativeAmount: 30,
  relativeUnit: "days",
  absoluteStart: "",
  absoluteEnd: "",
};

export function timeRangeToFilters(
  timeColumn: string,
  range: TimeRange,
): { column: string; operator: string; value: string }[] {
  const filters: { column: string; operator: string; value: string }[] = [];

  if (range.mode === "relative" && range.relativeAmount > 0) {
    const d = new Date();
    if (range.relativeUnit === "days") d.setDate(d.getDate() - range.relativeAmount);
    else if (range.relativeUnit === "weeks") d.setDate(d.getDate() - range.relativeAmount * 7);
    else if (range.relativeUnit === "months") d.setMonth(d.getMonth() - range.relativeAmount);
    filters.push({ column: timeColumn, operator: "gte", value: d.toISOString() });
  } else if (range.mode === "absolute") {
    if (range.absoluteStart) {
      filters.push({ column: timeColumn, operator: "gte", value: new Date(range.absoluteStart).toISOString() });
    }
    if (range.absoluteEnd) {
      filters.push({ column: timeColumn, operator: "lte", value: new Date(range.absoluteEnd).toISOString() });
    }
  }

  return filters;
}

interface TimeRangeSelectorProps {
  value: TimeRange;
  onChange: (range: TimeRange) => void;
  compact?: boolean;
}

export default function TimeRangeSelector({ value, onChange, compact }: TimeRangeSelectorProps) {
  return (
    <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", alignItems: "center" }}>
      <ToggleButtonGroup
        value={value.mode}
        exclusive
        onChange={(_, v) => { if (v) onChange({ ...value, mode: v }); }}
        size="small"
      >
        <ToggleButton value="relative">Relative</ToggleButton>
        <ToggleButton value="absolute">Absolute</ToggleButton>
      </ToggleButtonGroup>

      {value.mode === "relative" ? (
        <>
          <TextField
            size="small"
            label={compact ? "" : "Last"}
            type="number"
            value={value.relativeAmount || ""}
            onChange={(e) => onChange({ ...value, relativeAmount: parseInt(e.target.value) || 0 })}
            sx={{ width: 70 }}
          />
          <FormControl size="small" sx={{ minWidth: 90 }}>
            {!compact && <InputLabel>Unit</InputLabel>}
            <Select
              value={value.relativeUnit}
              label={compact ? undefined : "Unit"}
              onChange={(e) => onChange({
                ...value,
                relativeUnit: e.target.value as "days" | "weeks" | "months",
              })}
            >
              <MenuItem value="days">days</MenuItem>
              <MenuItem value="weeks">weeks</MenuItem>
              <MenuItem value="months">months</MenuItem>
            </Select>
          </FormControl>
        </>
      ) : (
        <>
          <TextField
            size="small"
            label="From"
            type="date"
            value={value.absoluteStart}
            onChange={(e) => onChange({ ...value, absoluteStart: e.target.value })}
            slotProps={{ inputLabel: { shrink: true } }}
            sx={{ width: 150 }}
          />
          <TextField
            size="small"
            label="To"
            type="date"
            value={value.absoluteEnd}
            onChange={(e) => onChange({ ...value, absoluteEnd: e.target.value })}
            slotProps={{ inputLabel: { shrink: true } }}
            sx={{ width: 150 }}
          />
        </>
      )}
    </Box>
  );
}
