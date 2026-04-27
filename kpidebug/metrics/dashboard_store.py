from abc import ABC, abstractmethod

from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import DashboardMetric, MetricSnapshot


class AbstractDashboardStore(ABC):
    @abstractmethod
    def add_metric(self, project_id: str, metric_id: str, aggregation: Aggregation = Aggregation.SUM) -> DashboardMetric:
        ...

    @abstractmethod
    def remove_metric(self, project_id: str, dashboard_metric_id: str) -> None:
        ...

    @abstractmethod
    def list_metrics(self, project_id: str) -> list[DashboardMetric]:
        ...

    @abstractmethod
    def store_snapshot(self, project_id: str, metric_id: str, snapshot: MetricSnapshot) -> None:
        ...
