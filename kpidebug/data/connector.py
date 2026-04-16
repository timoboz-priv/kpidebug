from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dataclass_field
from enum import Enum

from dataclasses_json import dataclass_json

from kpidebug.data.types import (
    DataRecord,
    DataSourceType,
    Dimension,
)


# --- Metrics ---

@dataclass_json
@dataclass
class MetricDescriptor:
    key: str = ""
    name: str = ""
    description: str = ""
    dimensions: list[Dimension] = dataclass_field(
        default_factory=list
    )


# --- Tables ---

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


@dataclass_json
@dataclass
class TableDescriptor:
    key: str = ""
    name: str = ""
    description: str = ""
    columns: list[TableColumn] = dataclass_field(
        default_factory=list
    )


# --- Connector ---

class DataSourceConnector(ABC):
    source_type: DataSourceType

    @abstractmethod
    def validate_credentials(
        self, credentials: dict[str, str],
    ) -> bool:
        ...

    # Metrics

    @abstractmethod
    def discover_metrics(self) -> list[MetricDescriptor]:
        """Return the metrics this data source provides."""
        ...

    @abstractmethod
    def fetch_metric_data(
        self,
        credentials: dict[str, str],
        metric_keys: list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[DataRecord]:
        """Fetch metric data live from the source.

        Returns DataRecords for aggregation by the caller.
        """
        ...

    # Tables

    @abstractmethod
    def discover_tables(self) -> list[TableDescriptor]:
        """Return the data tables this source provides."""
        ...

    @abstractmethod
    def fetch_table_data(
        self,
        credentials: dict[str, str],
        table_key: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict]:
        """Fetch raw table rows live from the source."""
        ...


class ConnectorError(Exception):
    pass
