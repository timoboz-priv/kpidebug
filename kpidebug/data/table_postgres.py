from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone

from psycopg_pool import ConnectionPool

from kpidebug.data.table import DataTable, GroupedTable, TableRow, matches_value
from kpidebug.data.table_memory import InMemoryDataTable
from kpidebug.data.types import (
    Aggregation,
    FilterOperator,
    Row,
    RowValue,
    TableDescriptor,
)


class PostgresDataTable(DataTable):
    _pool: ConnectionPool
    _source_id: str
    _table_key: str
    _schema: TableDescriptor
    _where_clauses: list[str]
    _where_params: list
    _order_by: str | None
    _limit_count: int | None
    _limit_offset: int
    _selected_columns: list[str] | None

    def __init__(
        self,
        pool: ConnectionPool,
        source_id: str,
        table_key: str,
        schema: TableDescriptor,
    ):
        self._pool = pool
        self._source_id = source_id
        self._table_key = table_key
        self._schema = schema
        self._where_clauses = []
        self._where_params = []
        self._order_by = None
        self._limit_count = None
        self._limit_offset = 0
        self._selected_columns = None

    def _clone(self) -> PostgresDataTable:
        c = PostgresDataTable(self._pool, self._source_id, self._table_key, self._schema)
        c._where_clauses = list(self._where_clauses)
        c._where_params = list(self._where_params)
        c._order_by = self._order_by
        c._limit_count = self._limit_count
        c._limit_offset = self._limit_offset
        c._selected_columns = list(self._selected_columns) if self._selected_columns else None
        return c

    def _base_where(self) -> tuple[str, list]:
        clauses = ["source_id = %s", "table_key = %s"] + self._where_clauses
        params: list = [self._source_id, self._table_key] + self._where_params
        return " AND ".join(clauses), params

    def _build_select(self, select_expr: str = "data") -> tuple[str, list]:
        where, params = self._base_where()
        sql = f"SELECT {select_expr} FROM cached_table_data WHERE {where}"
        if self._order_by:
            sql += f" {self._order_by}"
        else:
            sql += " ORDER BY row_index"
        if self._limit_count is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([self._limit_count, self._limit_offset])
        return sql, params

    # --- Schema ---

    def descriptor(self) -> TableDescriptor:
        return self._schema

    # --- Data access ---

    def count(self) -> int:
        where, params = self._base_where()
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM cached_table_data WHERE {where}", params,
            ).fetchone()
        return row[0] if row else 0

    def get(self, index: int) -> TableRow:
        rows = self.to_rows()
        return TableRow(_schema=self._schema, _data=rows[index])

    def rows(self) -> list[TableRow]:
        return [TableRow(_schema=self._schema, _data=r) for r in self.to_rows()]

    def to_rows(self) -> list[Row]:
        sql, params = self._build_select()
        with self._pool.connection() as conn:
            db_rows = conn.execute(sql, params).fetchall()
        result = []
        for r in db_rows:
            data = r[0] if isinstance(r[0], dict) else json.loads(r[0])
            if self._selected_columns:
                data = {k: v for k, v in data.items() if k in self._selected_columns}
            result.append(data)
        return result

    # --- Chainable operations ---

    def filter(self, field: str, operator: FilterOperator | str, value: RowValue) -> PostgresDataTable:
        c = self._clone()
        target = str(value) if value is not None else ""
        col_ref = f"data->>'{field}'"

        if operator == "eq":
            c._where_clauses.append(f"{col_ref} = %s")
            c._where_params.append(target)
        elif operator == "neq":
            c._where_clauses.append(f"{col_ref} != %s")
            c._where_params.append(target)
        elif operator == "contains":
            c._where_clauses.append(f"{col_ref} ILIKE %s")
            c._where_params.append(f"%{target}%")
        elif operator in ("gt", "gte", "lt", "lte"):
            sql_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[operator]
            try:
                numeric_val = float(target)
                c._where_clauses.append(f"({col_ref})::numeric {sql_op} %s")
                c._where_params.append(numeric_val)
            except ValueError:
                c._where_clauses.append(f"{col_ref} {sql_op} %s")
                c._where_params.append(target)
        return c

    def select(self, *columns: str) -> PostgresDataTable:
        c = self._clone()
        col_set = set(columns)
        new_cols = [col for col in self._schema.columns if col.key in col_set]
        c._schema = TableDescriptor(
            key=self._schema.key, name=self._schema.name, columns=new_cols,
        )
        c._selected_columns = list(columns)
        return c

    def limit(self, count: int, offset: int = 0) -> PostgresDataTable:
        c = self._clone()
        c._limit_count = count
        c._limit_offset = offset
        return c

    def sort(self, field: str, ascending: bool = True) -> PostgresDataTable:
        c = self._clone()
        direction = "ASC" if ascending else "DESC"
        c._order_by = f"ORDER BY data->>'{field}' {direction}"
        return c

    # --- Grouping & aggregation ---

    def group_by(self, *fields: str) -> GroupedTable:
        all_rows = self.to_rows()
        groups: dict[str, list[Row]] = {}
        for row in all_rows:
            key = "|".join(str(row.get(f, "")) for f in fields)
            groups.setdefault(key, []).append(row)
        tables = {
            key: InMemoryDataTable(self._schema, rows)
            for key, rows in groups.items()
        }
        return GroupedTable(_schema=self._schema, _groups=tables)

    def aggregate(self, field: str, method: Aggregation) -> float:
        where, params = self._base_where()
        if method == Aggregation.COUNT:
            sql = f"SELECT COUNT(*) FROM cached_table_data WHERE {where}"
        else:
            agg_fn = {
                Aggregation.SUM: "SUM",
                Aggregation.AVG: "AVG",
                Aggregation.MIN: "MIN",
                Aggregation.MAX: "MAX",
            }[method]
            col_expr = f"(data->>'{field}')::numeric"
            sql = f"SELECT COALESCE({agg_fn}({col_expr}), 0) FROM cached_table_data WHERE {where}"

        with self._pool.connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    # --- Joins ---

    def join(self, other: DataTable, on: str) -> InMemoryDataTable:
        mem = InMemoryDataTable(self._schema, self.to_rows())
        return mem.join(other, on)

    def union(self, other: DataTable) -> InMemoryDataTable:
        mem = InMemoryDataTable(self._schema, self.to_rows())
        return mem.union(other)

    # --- Static constructors ---

    @staticmethod
    def from_rows(
        pool: ConnectionPool,
        source_id: str,
        table_key: str,
        schema: TableDescriptor,
        rows: list[Row],
    ) -> PostgresDataTable:
        synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with pool.connection() as conn:
            conn.execute(
                "DELETE FROM cached_table_data WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )
            for i, row in enumerate(rows):
                conn.execute(
                    "INSERT INTO cached_table_data (source_id, table_key, row_index, data) VALUES (%s, %s, %s, %s)",
                    (source_id, table_key, i, json.dumps(row)),
                )
            conn.execute(
                """
                INSERT INTO cached_table_meta (source_id, table_key, synced_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_id, table_key) DO UPDATE SET synced_at = EXCLUDED.synced_at
                """,
                (source_id, table_key, synced_at),
            )
        return PostgresDataTable(pool, source_id, table_key, schema)

    @staticmethod
    def load(
        pool: ConnectionPool,
        source_id: str,
        table_key: str,
        schema: TableDescriptor,
    ) -> PostgresDataTable | None:
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM cached_table_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            ).fetchone()
        if row is None:
            return None
        return PostgresDataTable(pool, source_id, table_key, schema)
