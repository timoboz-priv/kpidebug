from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_data_source_store
from kpidebug.data.connector import ConnectorError
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import TableColumn, TableFilter, TableQuery
from kpidebug.management.types import ProjectMember, Role

from kpidebug.api.routes_data_sources import make_connector

router = APIRouter(
    prefix="/api/projects/{project_id}/data",
    tags=["data-tables"],
)


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


@router.post("/query")
def query_table(
    project_id: str,
    body: TableQueryRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
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

    source = data_source_store.get_source(project_id, body.source_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail="Data source not found",
        )

    if not source.credentials:
        raise HTTPException(
            status_code=400,
            detail="No credentials for this source",
        )

    connector = make_connector(source, data_source_store)

    tables = connector.get_tables()
    table_desc = next(
        (t for t in tables if t.key == body.table), None
    )
    if table_desc is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown table: {body.table}",
        )

    query = TableQuery(
        filters=body.filters,
        sort_by=body.sort_by,
        sort_order=body.sort_order,
        limit=body.limit,
        offset=body.offset,
    )

    try:
        result = connector.fetch_table_data(
            table_key=body.table,
            query=query,
        )
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    return TableQueryResponse(
        table=body.table,
        columns=table_desc.columns,
        rows=result.rows,
        total_count=result.total_count,
    )
