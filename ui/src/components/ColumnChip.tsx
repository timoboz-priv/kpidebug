import React from "react";
import { Chip, Tooltip } from "@mui/material";
import {
  TextFields as StringIcon,
  Tag as NumberIcon,
  AttachMoney as CurrencyIcon,
  Schedule as DatetimeIcon,
  ToggleOn as BooleanIcon,
  HelpOutlined as UnknownIcon,
} from "@mui/icons-material";
import { TableColumn } from "../api/dataSources";

const TYPE_ICONS: Record<string, React.ReactElement> = {
  string: <StringIcon fontSize="small" />,
  number: <NumberIcon fontSize="small" />,
  currency: <CurrencyIcon fontSize="small" />,
  datetime: <DatetimeIcon fontSize="small" />,
  boolean: <BooleanIcon fontSize="small" />,
};

export default function ColumnChip({ column }: { column: TableColumn }) {
  const icon = TYPE_ICONS[column.type] || <UnknownIcon fontSize="small" />;

  return (
    <Tooltip title={column.description || column.key} arrow>
      <Chip
        icon={icon}
        label={column.name}
        size="small"
        variant={column.is_primary_key ? "filled" : "outlined"}
        color={column.is_primary_key ? "primary" : "default"}
      />
    </Tooltip>
  );
}
