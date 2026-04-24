from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_data_source_store,
    require_project_role,
)
from kpidebug.data.cached_connector import CachedConnector, TableSyncError
from kpidebug.data.connector import (
    ConnectorError,
    DataSourceConnector,
)
from kpidebug.data.data_source_store import DataSourceStore
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.stripe.connector import StripeConnector
from kpidebug.data.google_analytics.connector import GoogleAnalyticsConnector
from kpidebug.data.types import DataSource, DataSourceType, TableDescriptor
from kpidebug.management.types import ProjectMember, Role

router = APIRouter(
    prefix="/api/projects/{project_id}/data-sources",
    tags=["data-sources"],
)

CONNECTOR_CLASSES: dict[DataSourceType, type[DataSourceConnector]] = {
    DataSourceType.STRIPE: StripeConnector,
    DataSourceType.GOOGLE_ANALYTICS: GoogleAnalyticsConnector,
}


def make_connector(
    source: DataSource,
    store: PostgresDataSourceStore,
) -> CachedConnector:
    connector_cls = CONNECTOR_CLASSES.get(source.type)
    if connector_cls is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {source.type.value}",
        )
    live = connector_cls(source)
    return CachedConnector(source, live, store)


@dataclass_json
@dataclass
class ConnectRequest:
    name: str = ""
    source_type: str = ""
    credentials: dict = None  # type: ignore[assignment]


@router.get("")
def list_data_sources(
    project_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_source_store: DataSourceStore = Depends(get_data_source_store),
) -> list[DataSource]:
    return data_source_store.list_sources(project_id)


@router.post("")
def connect_data_source(
    project_id: str,
    body: ConnectRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.ADMIN)
    ),
    data_source_store: DataSourceStore = Depends(get_data_source_store),
) -> DataSource:
    try:
        source_type = DataSourceType(body.source_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source type: {body.source_type}",
        )

    connector_cls = CONNECTOR_CLASSES.get(source_type)
    if connector_cls is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {body.source_type}",
        )

    temp_source = DataSource(type=source_type, credentials=body.credentials)
    connector = connector_cls(temp_source)

    try:
        connector.validate_credentials()
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    source = data_source_store.create_source(
        project_id=project_id,
        name=body.name,
        source_type=source_type,
        credentials=body.credentials,
    )

    return source


@router.delete("/{source_id}")
def disconnect_data_source(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.ADMIN)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> dict:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )
    data_source_store.clear_cached_source(source_id)
    data_source_store.delete_source(project_id, source_id)
    return {"ok": True}


@router.get("/{source_id}/tables")
def list_tables(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> list[TableDescriptor]:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )
    connector = make_connector(source, data_source_store)
    return connector.get_tables()


@dataclass_json
@dataclass
class SyncResponse:
    tables: dict[str, int] | None = None
    table: str = ""
    row_count: int = 0
    errors: list[TableSyncError] = dataclass_field(default_factory=list)


@router.post("/{source_id}/sync")
def sync_source(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.EDIT)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> SyncResponse:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )

    connector = make_connector(source, data_source_store)
    result = connector.sync_all()

    return SyncResponse(
        tables=result.tables,
        errors=result.errors,
    )


@router.post("/{source_id}/tables/{table_key}/sync")
def sync_table(
    project_id: str,
    source_id: str,
    table_key: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.EDIT)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> SyncResponse:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )

    connector = make_connector(source, data_source_store)

    tables = connector.get_tables()
    if not any(t.key == table_key for t in tables):
        raise HTTPException(
            status_code=404, detail=f"Unknown table: {table_key}"
        )

    try:
        rows = connector.sync_table(table_key)
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    return SyncResponse(table=table_key, row_count=len(rows))


# --- Metrics ---

from kpidebug.data.types import TableFilter
from kpidebug.metrics.builtin_metrics import (
    BuiltinMetric,
    MetricComputeResult,
    MetricDimension,
    builtin_registry,
)


@dataclass_json
@dataclass
class MetricDescriptor:
    key: str = ""
    name: str = ""
    description: str = ""
    data_type: str = ""
    has_custom_compute: bool = False
    dimensions: list[MetricDimension] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class MetricComputeRequest:
    group_by: list[str] = dataclass_field(default_factory=list)
    aggregation: str = "sum"
    filters: list[TableFilter] = dataclass_field(default_factory=list)
    time_column: str | None = None
    time_bucket: str | None = None


@dataclass_json
@dataclass
class MetricComputeResponse:
    metric_key: str = ""
    data_type: str = ""
    results: list[MetricComputeResult] = dataclass_field(default_factory=list)


@router.get("/{source_id}/metrics")
def list_metrics(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> list[MetricDescriptor]:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )

    connector = make_connector(source, data_source_store)
    table_keys = {t.key for t in connector.get_tables()}

    descriptors: list[MetricDescriptor] = []
    for metric in builtin_registry.list_all():
        if metric.table in table_keys:
            descriptors.append(MetricDescriptor(
                key=metric.key,
                name=metric.name,
                description=metric.description,
                data_type=metric.data_type,
                has_custom_compute=metric.compute_fn is not None,
                dimensions=metric.dimensions,
            ))
    return descriptors


@router.post("/{source_id}/metrics/{metric_key}/compute")
def compute_metric(
    project_id: str,
    source_id: str,
    metric_key: str,
    body: MetricComputeRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> MetricComputeResponse:
    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )

    metric = builtin_registry.get(metric_key)
    if metric is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown metric: {metric_key}"
        )

    connector = make_connector(source, data_source_store)
    all_rows = connector.fetch_all_rows(metric.table)

    results = builtin_registry.compute(
        metric_key, all_rows,
        group_by=body.group_by or None,
        aggregation=body.aggregation,
        filters=body.filters or None,
        time_column=body.time_column,
        time_bucket=body.time_bucket,
    )

    return MetricComputeResponse(
        metric_key=metric_key,
        data_type=metric.data_type,
        results=results,
    )
