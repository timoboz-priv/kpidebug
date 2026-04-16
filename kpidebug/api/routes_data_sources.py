from dataclasses import dataclass

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_data_store,
    require_project_role,
)
from kpidebug.data.connector import (
    ConnectorError,
    DataSourceConnector,
    MetricDescriptor,
    TableDescriptor,
)
from kpidebug.data.data_store import AbstractDataStore
from kpidebug.data.stripe.connector import StripeConnector
from kpidebug.data.types import DataSource, DataSourceType
from kpidebug.management.types import ProjectMember, Role

router = APIRouter(
    prefix="/api/projects/{project_id}/data-sources",
    tags=["data-sources"],
)

CONNECTORS: dict[DataSourceType, DataSourceConnector] = {
    DataSourceType.STRIPE: StripeConnector(),
}


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
    data_store: AbstractDataStore = Depends(get_data_store),
) -> list[DataSource]:
    return data_store.list_sources(project_id)


@router.post("")
def connect_data_source(
    project_id: str,
    body: ConnectRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.ADMIN)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
) -> DataSource:
    try:
        source_type = DataSourceType(body.source_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source type: {body.source_type}",
        )

    connector = CONNECTORS.get(source_type)
    if connector is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {body.source_type}",
        )

    try:
        connector.validate_credentials(body.credentials)
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    # Collect all dimensions from metrics
    metrics = connector.discover_metrics()
    all_dims = {}
    for m in metrics:
        for dim in m.dimensions:
            all_dims[dim.name] = dim
    dimensions = list(all_dims.values())

    source = data_store.create_source(
        project_id=project_id,
        name=body.name,
        source_type=source_type,
        dimensions=dimensions,
    )

    data_store.store_credentials(
        project_id, source.id, body.credentials
    )

    return source


@router.delete("/{source_id}")
def disconnect_data_source(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.ADMIN)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
) -> dict:
    source = data_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )
    data_store.delete_credentials(project_id, source_id)
    data_store.delete_source(project_id, source_id)
    return {"ok": True}


@router.get("/{source_id}/metrics")
def discover_metrics(
    project_id: str,
    source_id: str,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
) -> list[MetricDescriptor]:
    source = data_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(
            status_code=404, detail="Data source not found"
        )

    connector = CONNECTORS.get(source.type)
    if connector is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {source.type.value}",
        )

    return connector.discover_metrics()


@router.get("/{source_id}/tables")
def discover_tables(
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
            status_code=404, detail="Data source not found"
        )

    connector = CONNECTORS.get(source.type)
    if connector is None:
        raise HTTPException(
            status_code=400,
            detail=f"No connector for: {source.type.value}",
        )

    return connector.discover_tables()
