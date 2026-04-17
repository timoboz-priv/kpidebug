from dataclasses import dataclass, field as dataclass_field
from enum import Enum

from dataclasses_json import dataclass_json

from kpidebug.data.types import DataSourceType


@dataclass_json
@dataclass(frozen=True)
class DimensionValue:
    dimension: str = ""
    value: str = ""


@dataclass_json
@dataclass
class DataRecord:
    source_type: DataSourceType = DataSourceType.CUSTOM
    field: str = ""
    value: float = 0.0
    timestamp: str = ""
    dimension_values: list[DimensionValue] = dataclass_field(default_factory=list)


class MetricDataType(str, Enum):
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DURATION = "duration"
    RATE = "rate"


class MetricSource(str, Enum):
    BUILTIN = "builtin"
    AI_GENERATED = "ai_generated"
    DATA_SOURCE = "data_source"


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
    builtin_key: str = ""
    computation: str = ""
    source_filters: list[SourceFilter] = dataclass_field(default_factory=list)
    dimensions: list[str] = dataclass_field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


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
