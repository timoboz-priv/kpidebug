from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_data_store,
    get_table_cache,
    require_project_role,
)
from kpidebug.data.cache.base import TableCache
from kpidebug.data.connector import (
    ConnectorError,
    TableColumn,
    TableDescriptor,
)
from kpidebug.data.data_store import AbstractDataStore
from kpidebug.management.types import ProjectMember, Role

from kpidebug.api.routes_data_sources import CONNECTORS

router = APIRouter(
    prefix="/api/projects/{project_id}/data",
    tags=["data-tables"],
)


@dataclass_json
@dataclass
class TableFilter:
    column: str = ""
    operator: str = "eq"
    value: str = ""


@dataclass_json
@dataclass
class TableQueryRequest:
    source_id: str = ""
    table: str = ""
    filters: list[TableFilter] = dataclass_field(
        default_factory=list
    )
    sort_by: str | None = None
    sort_order: str = "asc"
    limit: int = 100
    offset: int = 0


@dataclass_json
@dataclass
class TableQueryResponse:
    table: str = ""
    columns: list[TableColumn] = dataclass_field(
        default_factory=list
    )
    rows: list[dict] = dataclass_field(
        default_factory=list
    )
    total_count: int = 0
    from_cache: bool = False


@dataclass_json
@dataclass
class SyncRequest:
    source_id: str = ""
    table: str = ""


@dataclass_json
@dataclass
class SyncResponse:
    table: str = ""
    row_count: int = 0


@router.post("/query")
def query_table(
    project_id: str,
    body: TableQueryRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
    table_cache: TableCache | None = Depends(
        get_table_cache
    ),
) -> TableQueryResponse:
    if not body.source_id:
        raise HTTPException(
            status_code=400,
            detail="source_id is required",
        )
    if not body.table:
        raise HTTPException(
            status_code=400,
            detail="table is required",
        )

    source, connector, credentials, table_desc = (
        _resolve_table(
            data_store, project_id,
            body.source_id, body.table,
        )
    )

    # Try cache first
    from_cache = False
    if table_cache and table_cache.is_cached(
        body.source_id, body.table
    ):
        rows = table_cache.get_rows(
            body.source_id, body.table
        )
        if rows is not None:
            from_cache = True

    if not from_cache:
        try:
            rows = connector.fetch_table_data(
                credentials=credentials,
                table_key=body.table,
            )
        except ConnectorError as e:
            raise HTTPException(
                status_code=400, detail=str(e)
            )

        # Auto-populate cache on first fetch
        if table_cache:
            pk_cols = [
                c.key for c in table_desc.columns
                if c.is_primary_key
            ]
            table_cache.set_rows(
                body.source_id, body.table, rows
            )

    # Apply filters
    if body.filters:
        rows = _apply_table_filters(rows, body.filters)

    total_count = len(rows)

    # Sort
    if body.sort_by:
        reverse = body.sort_order == "desc"
        rows = sorted(
            rows,
            key=lambda r: r.get(body.sort_by, ""),
            reverse=reverse,
        )

    # Paginate
    rows = rows[body.offset:body.offset + body.limit]

    return TableQueryResponse(
        table=body.table,
        columns=table_desc.columns,
        rows=rows,
        total_count=total_count,
        from_cache=from_cache,
    )


@router.post("/sync")
def sync_table(
    project_id: str,
    body: SyncRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.EDIT)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
    table_cache: TableCache | None = Depends(
        get_table_cache
    ),
) -> SyncResponse:
    if not body.source_id:
        raise HTTPException(
            status_code=400,
            detail="source_id is required",
        )
    if not body.table:
        raise HTTPException(
            status_code=400,
            detail="table is required",
        )

    source, connector, credentials, table_desc = (
        _resolve_table(
            data_store, project_id,
            body.source_id, body.table,
        )
    )

    # Fetch fresh data from source
    try:
        fresh_rows = connector.fetch_table_data(
            credentials=credentials,
            table_key=body.table,
        )
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    # Sync cache using primary keys
    if table_cache:
        pk_cols = [
            c.key for c in table_desc.columns
            if c.is_primary_key
        ]
        table_cache.sync_rows(
            body.source_id, body.table,
            fresh_rows, pk_cols,
        )

    return SyncResponse(
        table=body.table,
        row_count=len(fresh_rows),
    )


@router.get("/tables/{source_id}")
def list_tables(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
) -> list[TableDescriptor]:
    source = data_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail="Data source not found",
        )
    connector = CONNECTORS.get(source.type)
    if connector is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {source.type.value}",
        )
    return connector.discover_tables()


def _resolve_table(
    data_store: AbstractDataStore,
    project_id: str,
    source_id: str,
    table_key: str,
) -> tuple:
    source = data_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail="Data source not found",
        )

    connector = CONNECTORS.get(source.type)
    if connector is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {source.type.value}",
        )

    credentials = source.credentials
    if not credentials:
        raise HTTPException(
            status_code=400,
            detail="No credentials for this source",
        )

    tables = connector.discover_tables()
    table_desc = next(
        (t for t in tables if t.key == table_key), None
    )
    if table_desc is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown table: {table_key}",
        )

    return source, connector, credentials, table_desc


def _apply_table_filters(
    rows: list[dict], filters: list[TableFilter],
) -> list[dict]:
    filtered: list[dict] = []
    for row in rows:
        if all(_matches(row, f) for f in filters):
            filtered.append(row)
    return filtered


def _matches(row: dict, f: TableFilter) -> bool:
    val = str(row.get(f.column, ""))
    target = f.value

    if f.operator == "eq":
        return val == target
    elif f.operator == "neq":
        return val != target
    elif f.operator == "contains":
        return target.lower() in val.lower()
    elif f.operator == "gt":
        try:
            return float(val) > float(target)
        except ValueError:
            return val > target
    elif f.operator == "gte":
        try:
            return float(val) >= float(target)
        except ValueError:
            return val >= target
    elif f.operator == "lt":
        try:
            return float(val) < float(target)
        except ValueError:
            return val < target
    elif f.operator == "lte":
        try:
            return float(val) <= float(target)
        except ValueError:
            return val <= target

    return True
