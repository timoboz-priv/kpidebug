from __future__ import annotations

import csv

from kpidebug.data.table_memory import InMemoryDataTable
from kpidebug.data.types import ColumnType, TableColumn, TableDescriptor


class CsvDataTable(InMemoryDataTable):
    def __init__(self, file_path: str, table_key: str = "", table_name: str = ""):
        rows, columns = _load_csv(file_path)
        schema = TableDescriptor(
            key=table_key or file_path,
            name=table_name or file_path,
            columns=columns,
        )
        super().__init__(schema, rows)


def _load_csv(path: str) -> tuple[list[dict], list[TableColumn]]:
    rows: list[dict] = []
    columns: list[TableColumn] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    if not raw_rows:
        return [], []

    for key in raw_rows[0].keys():
        col_type = _infer_type(raw_rows, key)
        columns.append(TableColumn(key=key, name=key, type=col_type))

    for raw in raw_rows:
        row: dict = {}
        for col in columns:
            val = raw.get(col.key, "")
            if col.type == ColumnType.NUMBER or col.type == ColumnType.CURRENCY:
                try:
                    row[col.key] = float(val) if "." in str(val) else int(val)
                except (ValueError, TypeError):
                    row[col.key] = val
            elif col.type == ColumnType.BOOLEAN:
                row[col.key] = val.lower() in ("true", "1", "yes")
            else:
                row[col.key] = val
        rows.append(row)

    return rows, columns


def _infer_type(rows: list[dict], key: str) -> ColumnType:
    sample = [r.get(key, "") for r in rows[:20] if r.get(key, "") != ""]
    if not sample:
        return ColumnType.STRING

    num_count = 0
    bool_count = 0
    for val in sample:
        s = str(val).strip()
        if s.lower() in ("true", "false", "yes", "no", "1", "0"):
            bool_count += 1
        try:
            float(s)
            num_count += 1
        except ValueError:
            pass

    if bool_count == len(sample):
        return ColumnType.BOOLEAN
    if num_count == len(sample):
        return ColumnType.NUMBER
    return ColumnType.STRING
