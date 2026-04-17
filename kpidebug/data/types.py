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


class ColumnType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    CURRENCY = "currency"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


@dataclass_json
@dataclass
class TableColumn:
    key: str = ""
    name: str = ""
    description: str = ""
    type: ColumnType = ColumnType.STRING
    is_primary_key: bool = False
    annotations: list[str] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class TableDescriptor:
    key: str = ""
    name: str = ""
    description: str = ""
    columns: list[TableColumn] = dataclass_field(
        default_factory=list
    )


@dataclass_json
@dataclass
class TableFilter:
    column: str = ""
    operator: str = "eq"
    value: str = ""


@dataclass_json
@dataclass
class TableQuery:
    filters: list[TableFilter] = dataclass_field(default_factory=list)
    sort_by: str | None = None
    sort_order: str = "asc"
    limit: int = 100
    offset: int = 0


@dataclass_json
@dataclass
class TableResult:
    rows: list[dict] = dataclass_field(default_factory=list)
    total_count: int = 0


@dataclass_json
@dataclass
class DataSource:
    id: str = ""
    project_id: str = ""
    name: str = ""
    type: DataSourceType = DataSourceType.CUSTOM
    credentials: dict[str, str] = dataclass_field(default_factory=dict)
