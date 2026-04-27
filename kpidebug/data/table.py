from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dataclass_field

from kpidebug.data.types import (
    Aggregation,
    FilterOperator,
    Row,
    RowValue,
    SortOrder,
    TableDescriptor,
    TableFilter,
    TableQuery,
)


@dataclass
class TableRow:
    _schema: TableDescriptor
    _data: Row

    def get(self, field_name: str) -> RowValue:
        return self._data.get(field_name)

    def __getitem__(self, field_name: str) -> RowValue:
        return self._data[field_name]

    def to_dict(self) -> Row:
        return dict(self._data)

    @property
    def schema(self) -> TableDescriptor:
        return self._schema


class DataTable(ABC):
    @abstractmethod
    def descriptor(self) -> TableDescriptor:
        ...

    @abstractmethod
    def count(self) -> int:
        ...

    @abstractmethod
    def get(self, index: int) -> TableRow:
        ...

    @abstractmethod
    def rows(self) -> list[TableRow]:
        ...

    @abstractmethod
    def to_rows(self) -> list[Row]:
        ...

    @abstractmethod
    def filter(self, field: str, operator: FilterOperator | str, value: RowValue) -> DataTable:
        ...

    @abstractmethod
    def select(self, *columns: str) -> DataTable:
        ...

    @abstractmethod
    def limit(self, count: int, offset: int = 0) -> DataTable:
        ...

    @abstractmethod
    def sort(self, field: str, ascending: bool = True) -> DataTable:
        ...

    @abstractmethod
    def group_by(self, *fields: str) -> GroupedTable:
        ...

    @abstractmethod
    def aggregate(self, field: str, method: Aggregation) -> float:
        ...

    @abstractmethod
    def join(self, other: DataTable, on: str) -> DataTable:
        ...

    @abstractmethod
    def union(self, other: DataTable) -> DataTable:
        ...

    def query(self, q: TableQuery) -> DataTable:
        result: DataTable = self
        for f in q.filters:
            result = result.filter(f.column, f.operator, f.value)
        if q.sort_by:
            result = result.sort(q.sort_by, ascending=(q.sort_order != SortOrder.DESC))
        result = result.limit(q.limit, q.offset)
        return result

    def filter_rows(self, predicate: ...) -> DataTable:
        from kpidebug.data.table_memory import InMemoryDataTable
        kept = [r for r in self.to_rows() if predicate(r)]
        return InMemoryDataTable(self.descriptor(), kept)

    def add_column(self, key: str, name: str, compute: ...) -> DataTable:
        from kpidebug.data.table_memory import InMemoryDataTable
        from kpidebug.data.types import ColumnType, TableColumn
        new_cols = list(self.descriptor().columns) + [
            TableColumn(key=key, name=name, type=ColumnType.STRING),
        ]
        new_schema = TableDescriptor(
            key=self.descriptor().key,
            name=self.descriptor().name,
            columns=new_cols,
        )
        new_rows = []
        for row in self.to_rows():
            r = dict(row)
            r[key] = compute(row)
            new_rows.append(r)
        return InMemoryDataTable(new_schema, new_rows)


@dataclass
class GroupedTable:
    _schema: TableDescriptor
    _groups: dict[str, DataTable] = dataclass_field(default_factory=dict)

    @property
    def schema(self) -> TableDescriptor:
        return self._schema

    @property
    def groups(self) -> dict[str, DataTable]:
        return self._groups

    def aggregate(self, field: str, method: Aggregation) -> dict[str, float]:
        return {key: table.aggregate(field, method) for key, table in self._groups.items()}

    def keys(self) -> list[str]:
        return list(self._groups.keys())

    def __getitem__(self, key: str) -> DataTable:
        return self._groups[key]

    def __len__(self) -> int:
        return len(self._groups)


def matches_value(row_val: RowValue, operator: FilterOperator | str, target: str) -> bool:
    val = str(row_val) if row_val is not None else ""
    op = FilterOperator(operator) if isinstance(operator, str) else operator
    if op == FilterOperator.EQ:
        return val == target
    elif op == FilterOperator.NEQ:
        return val != target
    elif op == FilterOperator.CONTAINS:
        return target.lower() in val.lower()
    elif op in (FilterOperator.GT, FilterOperator.GTE, FilterOperator.LT, FilterOperator.LTE):
        try:
            fval, ftarget = float(val), float(target)
        except ValueError:
            fval, ftarget = None, None
        if fval is not None and ftarget is not None:
            if op == FilterOperator.GT:
                return fval > ftarget
            elif op == FilterOperator.GTE:
                return fval >= ftarget
            elif op == FilterOperator.LT:
                return fval < ftarget
            elif op == FilterOperator.LTE:
                return fval <= ftarget
        return val > target if op in (FilterOperator.GT, FilterOperator.GTE) else val < target
    return True
