from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_data_store,
    require_project_role,
)
from kpidebug.data.connector import ConnectorError
from kpidebug.data.data_store import AbstractDataStore
from kpidebug.data.types import DataRecord
from kpidebug.management.types import ProjectMember, Role

from kpidebug.api.routes_data_sources import CONNECTORS

router = APIRouter(
    prefix="/api/projects/{project_id}/metrics",
    tags=["metric-explore"],
)


@dataclass_json
@dataclass
class DimensionFilter:
    dimension: str = ""
    value: str = ""


@dataclass_json
@dataclass
class MetricExploreRequest:
    source_id: str = ""
    metric_key: str = ""
    aggregation: str = "sum"
    group_by: str | None = None
    filters: list[DimensionFilter] = dataclass_field(
        default_factory=list
    )
    start_time: str | None = None
    end_time: str | None = None


@dataclass_json
@dataclass
class ExploreResultRow:
    value: float = 0.0
    group: str | None = None


@dataclass_json
@dataclass
class MetricExploreResponse:
    metric_key: str = ""
    aggregation: str = ""
    results: list[ExploreResultRow] = dataclass_field(
        default_factory=list
    )
    record_count: int = 0


AGGREGATION_METHODS = {"sum", "count", "avg", "min", "max"}


@router.post("/explore")
def explore_metric(
    project_id: str,
    body: MetricExploreRequest,
    _member: ProjectMember = Depends(
        require_project_role(Role.READ)
    ),
    data_store: AbstractDataStore = Depends(get_data_store),
) -> MetricExploreResponse:
    if body.aggregation not in AGGREGATION_METHODS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown aggregation: {body.aggregation}. "
                f"Must be one of: "
                f"{', '.join(sorted(AGGREGATION_METHODS))}"
            ),
        )

    if not body.metric_key:
        raise HTTPException(
            status_code=400,
            detail="metric_key is required",
        )

    if not body.source_id:
        raise HTTPException(
            status_code=400,
            detail="source_id is required",
        )

    # Look up source and connector
    source = data_store.get_source(
        project_id, body.source_id
    )
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

    credentials = data_store.get_credentials(
        project_id, body.source_id
    )
    if credentials is None:
        raise HTTPException(
            status_code=400,
            detail="No credentials for this source",
        )

    # Fetch metric data live from the source
    try:
        records = connector.fetch_metric_data(
            credentials=credentials,
            metric_keys=[body.metric_key],
            start_time=body.start_time,
            end_time=body.end_time,
        )
    except ConnectorError as e:
        raise HTTPException(
            status_code=400, detail=str(e)
        )

    # Apply dimension filters
    if body.filters:
        records = _apply_filters(records, body.filters)

    # Group and aggregate
    if body.group_by:
        groups = _group_records(records, body.group_by)
        results = [
            ExploreResultRow(
                value=_aggregate(
                    group_records, body.aggregation
                ),
                group=group_key,
            )
            for group_key, group_records
            in sorted(groups.items())
        ]
    else:
        results = [
            ExploreResultRow(
                value=_aggregate(
                    records, body.aggregation
                ),
                group=None,
            )
        ]

    return MetricExploreResponse(
        metric_key=body.metric_key,
        aggregation=body.aggregation,
        results=results,
        record_count=len(records),
    )


def _apply_filters(
    records: list[DataRecord],
    filters: list[DimensionFilter],
) -> list[DataRecord]:
    filtered: list[DataRecord] = []
    for record in records:
        dim_map = {
            dv.dimension: dv.value
            for dv in record.dimension_values
        }
        match = all(
            dim_map.get(f.dimension) == f.value
            for f in filters
        )
        if match:
            filtered.append(record)
    return filtered


def _group_records(
    records: list[DataRecord], group_by: str,
) -> dict[str, list[DataRecord]]:
    groups: dict[str, list[DataRecord]] = {}
    for record in records:
        dim_map = {
            dv.dimension: dv.value
            for dv in record.dimension_values
        }
        key = dim_map.get(group_by, "(none)")
        groups.setdefault(key, []).append(record)
    return groups


def _aggregate(
    records: list[DataRecord], method: str,
) -> float:
    if not records:
        return 0.0

    values = [r.value for r in records]

    if method == "sum":
        return sum(values)
    elif method == "count":
        return float(len(values))
    elif method == "avg":
        return sum(values) / len(values)
    elif method == "min":
        return min(values)
    elif method == "max":
        return max(values)

    return 0.0
