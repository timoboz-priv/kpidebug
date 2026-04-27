from __future__ import annotations

from kpidebug.data.table import DataTable, GroupedTable, TableRow, matches_value
from kpidebug.data.types import (
    Aggregation,
    FilterOperator,
    Row,
    RowValue,
    TableDescriptor,
)


class InMemoryDataTable(DataTable):
    _descriptor: TableDescriptor
    _rows: list[Row]

    def __init__(self, schema: TableDescriptor, rows: list[Row] | None = None):
        self._descriptor = schema
        self._rows = list(rows) if rows else []

    def descriptor(self) -> TableDescriptor:
        return self._descriptor

    def count(self) -> int:
        return len(self._rows)

    def get(self, index: int) -> TableRow:
        return TableRow(_schema=self._descriptor, _data=self._rows[index])

    def rows(self) -> list[TableRow]:
        return [TableRow(_schema=self._descriptor, _data=r) for r in self._rows]

    def to_rows(self) -> list[Row]:
        return list(self._rows)

    def filter(self, field: str, operator: FilterOperator | str, value: RowValue) -> InMemoryDataTable:
        target = str(value) if value is not None else ""
        kept = [r for r in self._rows if matches_value(r.get(field), operator, target)]
        return InMemoryDataTable(self._descriptor, kept)

    def select(self, *columns: str) -> InMemoryDataTable:
        col_set = set(columns)
        new_cols = [c for c in self._descriptor.columns if c.key in col_set]
        new_schema = TableDescriptor(
            key=self._descriptor.key,
            name=self._descriptor.name,
            columns=new_cols,
        )
        new_rows = [{k: v for k, v in r.items() if k in col_set} for r in self._rows]
        return InMemoryDataTable(new_schema, new_rows)

    def limit(self, count: int, offset: int = 0) -> InMemoryDataTable:
        return InMemoryDataTable(self._descriptor, self._rows[offset:offset + count])

    def sort(self, field: str, ascending: bool = True) -> InMemoryDataTable:
        def sort_key(row: Row) -> tuple[int, float | str]:
            val = row.get(field)
            if val is None:
                return (1, "")
            try:
                return (0, float(val))
            except (ValueError, TypeError):
                return (0, str(val))

        sorted_rows = sorted(self._rows, key=sort_key, reverse=not ascending)
        return InMemoryDataTable(self._descriptor, sorted_rows)

    def group_by(self, *fields: str) -> GroupedTable:
        groups: dict[str, list[Row]] = {}
        for row in self._rows:
            key = "|".join(str(row.get(f, "")) for f in fields)
            groups.setdefault(key, []).append(row)
        tables: dict[str, DataTable] = {
            key: InMemoryDataTable(self._descriptor, rows)
            for key, rows in groups.items()
        }
        return GroupedTable(_schema=self._descriptor, _groups=tables)

    def aggregate(self, field: str, method: Aggregation) -> float:
        if method == Aggregation.COUNT:
            return float(len(self._rows))

        values: list[float] = []
        for r in self._rows:
            val = r.get(field)
            if val is None:
                continue
            try:
                values.append(float(val))
            except (ValueError, TypeError):
                continue

        if not values:
            return 0.0

        if method == Aggregation.SUM:
            return sum(values)
        elif method == Aggregation.AVG:
            return sum(values) / len(values)
        elif method == Aggregation.MIN:
            return min(values)
        elif method == Aggregation.MAX:
            return max(values)
        return 0.0

    def join(self, other: DataTable, on: str) -> InMemoryDataTable:
        self_cols = {c.key for c in self._descriptor.columns}

        merged_columns = list(self._descriptor.columns)
        for c in other.descriptor().columns:
            if c.key not in self_cols:
                merged_columns.append(c)

        merged_schema = TableDescriptor(
            key=self._descriptor.key,
            name=self._descriptor.name,
            columns=merged_columns,
        )

        index: dict[str, list[Row]] = {}
        for r in other.to_rows():
            key = str(r.get(on, ""))
            index.setdefault(key, []).append(r)

        result_rows: list[Row] = []
        for left in self._rows:
            key = str(left.get(on, ""))
            for right in index.get(key, []):
                merged: Row = dict(left)
                for k, v in right.items():
                    if k != on and k not in self_cols:
                        merged[k] = v
                result_rows.append(merged)

        return InMemoryDataTable(merged_schema, result_rows)

    def union(self, other: DataTable) -> InMemoryDataTable:
        return InMemoryDataTable(self._descriptor, self._rows + other.to_rows())
