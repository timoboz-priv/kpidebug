from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import TYPE_CHECKING

from dataclasses_json import dataclass_json

from kpidebug.data.types import DataSourceType, Row, TableFilter

if TYPE_CHECKING:
    from kpidebug.data.connector import DataSourceConnector
    from kpidebug.data.types import DataSource


class MetricDataType(str, Enum):
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DURATION = "duration"
    RATE = "rate"


class MetricSource(str, Enum):
    BUILTIN = "builtin"
    EXPRESSION = "expression"


class Aggregation(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class TimeBucket(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


@dataclass_json
@dataclass(frozen=True)
class DimensionValue:
    dimension: str = ""
    value: str = ""


@dataclass_json
@dataclass
class SourceFilter:
    source_type: DataSourceType = DataSourceType.CUSTOM
    fields: list[str] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class MetricDefinition:
    id: str = ""
    project_id: str = ""
    name: str = ""
    description: str = ""
    data_type: MetricDataType = MetricDataType.NUMBER
    source: MetricSource = MetricSource.BUILTIN
    source_id: str = ""
    table: str = ""
    builtin_key: str = ""
    value_field: str = ""
    aggregation: Aggregation = Aggregation.SUM
    computation: str = ""
    source_filters: list[SourceFilter] = dataclass_field(default_factory=list)
    dimensions: list[str] = dataclass_field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


@dataclass_json
@dataclass
class MetricDefinitionUpdate:
    name: str | None = None
    description: str | None = None
    data_type: str | None = None
    computation: str | None = None
    dimensions: list[str] | None = None

    def to_db_fields(self) -> dict[str, str | list[str]]:
        fields: dict[str, str | list[str]] = {}
        if self.name is not None:
            fields["name"] = self.name
        if self.description is not None:
            fields["description"] = self.description
        if self.data_type is not None:
            fields["data_type"] = self.data_type
        if self.computation is not None:
            fields["computation"] = self.computation
        if self.dimensions is not None:
            import json
            fields["dimensions"] = json.dumps(self.dimensions)
        return fields


@dataclass_json
@dataclass
class MetricResult:
    id: str = ""
    metric_id: str = ""
    project_id: str = ""
    value: float = 0.0
    dimension_values: list[DimensionValue] = dataclass_field(default_factory=list)
    computed_at: str = ""
    period_start: str = ""
    period_end: str = ""


@dataclass_json
@dataclass
class DashboardMetric:
    id: str = ""
    project_id: str = ""
    metric_id: str = ""
    position: int = 0
    added_at: str = ""


@dataclass
class MetricComputeInput:
    rows: list[Row] = dataclass_field(default_factory=list)
    source_id: str = ""
    group_by: list[str] = dataclass_field(default_factory=list)
    aggregation: Aggregation = Aggregation.SUM
    filters: list[TableFilter] = dataclass_field(default_factory=list)
    time_column: str | None = None
    time_bucket: TimeBucket | None = None


@dataclass
class SourceConnectorPair:
    source: DataSource
    connector: DataSourceConnector
