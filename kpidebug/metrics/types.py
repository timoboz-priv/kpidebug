from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING

from dataclasses_json import dataclass_json

from kpidebug.data.types import Aggregation, FilterOperator, TableFilter

if TYPE_CHECKING:
    from kpidebug.data.table import DataTable
    from kpidebug.metrics.context import MetricContext


class MetricDataType(str, Enum):
    NUMBER = "number"
    PERCENT = "percent"
    CURRENCY = "currency"
    DURATION = "duration"
    RATE = "rate"


class TimeBucket(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


@dataclass_json
@dataclass
class MetricDimension:
    key: str = ""
    name: str = ""


@dataclass_json
@dataclass
class MetricResult:
    value: float = 0.0
    groups: dict[str, str] = dataclass_field(default_factory=dict)


@dataclass_json
@dataclass
class MetricSeriesPoint:
    date: date = dataclass_field(default_factory=lambda: date.today())
    results: list[MetricResult] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class MetricSeriesResult:
    points: list[MetricSeriesPoint] = dataclass_field(default_factory=list)

    @property
    def dates(self) -> list[date]:
        return [p.date for p in self.points]

    @property
    def values(self) -> list[float]:
        return [p.results[0].value if p.results else 0.0 for p in self.points]


@dataclass_json
@dataclass(frozen=True)
class DimensionValue:
    dimension: str = ""
    value: str = ""


@dataclass_json
@dataclass
class MetricDefinition:
    id: str = ""
    project_id: str = ""
    name: str = ""
    description: str = ""
    data_type: MetricDataType = MetricDataType.NUMBER
    computation: str = ""
    created_at: datetime = dataclass_field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = dataclass_field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass_json
@dataclass
class MetricDefinitionUpdate:
    name: str | None = None
    description: str | None = None
    data_type: MetricDataType | None = None
    computation: str | None = None

    def to_db_fields(self) -> dict[str, str]:
        fields: dict[str, str] = {}
        if self.name is not None:
            fields["name"] = self.name
        if self.description is not None:
            fields["description"] = self.description
        if self.data_type is not None:
            fields["data_type"] = self.data_type.value
        if self.computation is not None:
            fields["computation"] = self.computation
        return fields


@dataclass_json
@dataclass
class StoredMetricResult:
    id: str = ""
    metric_id: str = ""
    project_id: str = ""
    value: float = 0.0
    dimension_values: list[DimensionValue] = dataclass_field(default_factory=list)
    computed_at: datetime = dataclass_field(default_factory=lambda: datetime.now(timezone.utc))
    period_start: date = dataclass_field(default_factory=lambda: date.today())
    period_end: date = dataclass_field(default_factory=lambda: date.today())


@dataclass_json
@dataclass
class DashboardMetric:
    id: str = ""
    project_id: str = ""
    metric_id: str = ""
    position: int = 0
    added_at: datetime = dataclass_field(default_factory=lambda: datetime.now(timezone.utc))
    snapshot: MetricSnapshot | None = None


@dataclass_json
@dataclass
class MetricSnapshot:
    id: str = ""
    metric_id: str = ""
    project_id: str = ""
    date: date = dataclass_field(default_factory=lambda: date.today())
    values: list[float] = dataclass_field(default_factory=list)

    @property
    def value(self) -> float:
        return self.values[-1] if self.values else 0.0

    def change(self, y: int) -> float:
        if len(self.values) < 2 * y:
            return 0.0
        recent = self.values[-y:]
        previous = self.values[-2 * y:-y]
        avg_previous = sum(previous) / len(previous)
        if avg_previous == 0:
            return 0.0
        avg_recent = sum(recent) / len(recent)
        return (avg_recent - avg_previous) / avg_previous


class Metric(ABC):
    id: str
    name: str
    description: str
    data_type: MetricDataType
    dimensions: list[MetricDimension] = []
    table_keys: list[str] = []

    @abstractmethod
    def compute_single(
        self,
        ctx: MetricContext,
        dimensions: list[str] | None = None,
        aggregation: Aggregation = Aggregation.SUM,
        filters: list[TableFilter] | None = None,
        days: int = 30,
        date: date | None = None,
    ) -> list[MetricResult]:
        ...

    def compute_series(
        self,
        ctx: MetricContext,
        dimensions: list[str] | None = None,
        aggregation: Aggregation = Aggregation.SUM,
        filters: list[TableFilter] | None = None,
        days: int = 30,
        date: date | None = None,
        time_bucket: TimeBucket = TimeBucket.DAY,
    ) -> MetricSeriesResult:
        end = date or datetime.now(timezone.utc).date()
        points: list[MetricSeriesPoint] = []

        if time_bucket == TimeBucket.DAY:
            for i in range(days, 0, -1):
                day = end - timedelta(days=i - 1)
                results = self.compute_single(
                    ctx, dimensions=dimensions, aggregation=aggregation,
                    filters=filters, days=1, date=day,
                )
                points.append(MetricSeriesPoint(date=day, results=results))
        elif time_bucket == TimeBucket.WEEK:
            current = end - timedelta(days=days - 1)
            current = current - timedelta(days=current.weekday())
            while current <= end:
                week_end = current + timedelta(days=6)
                bucket_days = min((week_end - current).days + 1, (end - current).days + 1)
                results = self.compute_single(
                    ctx, dimensions=dimensions, aggregation=aggregation,
                    filters=filters, days=bucket_days, date=week_end,
                )
                points.append(MetricSeriesPoint(date=current, results=results))
                current += timedelta(weeks=1)
        elif time_bucket == TimeBucket.MONTH:
            from calendar import monthrange
            current = (end - timedelta(days=days - 1)).replace(day=1)
            while current <= end:
                month_days = monthrange(current.year, current.month)[1]
                month_end = current.replace(day=month_days)
                actual_end = min(month_end, end)
                bucket_days = (actual_end - current).days + 1
                results = self.compute_single(
                    ctx, dimensions=dimensions, aggregation=aggregation,
                    filters=filters, days=bucket_days, date=actual_end,
                )
                points.append(MetricSeriesPoint(date=current, results=results))
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

        return MetricSeriesResult(points=points)


def apply_time_filter(
    table: DataTable,
    time_column: str,
    days: int,
    date: date | None = None,
) -> DataTable:
    end = date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)
    end_next = end + timedelta(days=1)
    table = table.filter(time_column, FilterOperator.GTE, start.isoformat() + "T00:00:00Z")
    table = table.filter(time_column, FilterOperator.LT, end_next.isoformat() + "T00:00:00Z")
    return table


def parse_group_key(key: str, dimensions: list[str]) -> dict[str, str]:
    parts = key.split("|")
    return {dim: parts[i] if i < len(parts) else "" for i, dim in enumerate(dimensions)}
