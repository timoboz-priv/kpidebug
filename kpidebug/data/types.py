from dataclasses import dataclass, field as dataclass_field
from enum import Enum

from dataclasses_json import dataclass_json


class DataSourceType(str, Enum):
    STRIPE = "stripe"
    GOOGLE_ANALYTICS = "google_analytics"
    GOOGLE_CLOUD = "google_cloud"
    DATADOG = "datadog"
    TABLEAU = "tableau"
    CUSTOM = "custom"


class DimensionType(str, Enum):
    TEMPORAL = "temporal"
    CATEGORICAL = "categorical"


@dataclass_json
@dataclass
class Dimension:
    name: str = ""
    type: DimensionType = DimensionType.CATEGORICAL


@dataclass_json
@dataclass(frozen=True)
class DimensionValue:
    dimension: str = ""
    value: str = ""


@dataclass_json
@dataclass
class DataSource:
    id: str = ""
    project_id: str = ""
    name: str = ""
    type: DataSourceType = DataSourceType.CUSTOM
    dimensions: list[Dimension] = dataclass_field(default_factory=list)
    credentials: dict[str, str] = dataclass_field(default_factory=dict)


@dataclass_json
@dataclass
class DataRecord:
    """Internal type for metric aggregation. Not persisted."""
    source_type: DataSourceType = DataSourceType.CUSTOM
    field: str = ""
    value: float = 0.0
    timestamp: str = ""
    dimension_values: list[DimensionValue] = dataclass_field(default_factory=list)
