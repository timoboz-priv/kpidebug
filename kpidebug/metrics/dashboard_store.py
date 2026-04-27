from abc import ABC, abstractmethod

from kpidebug.metrics.types import DashboardMetric


class AbstractDashboardStore(ABC):
    @abstractmethod
    def add_metric(self, project_id: str, metric_id: str) -> DashboardMetric:
        ...

    @abstractmethod
    def remove_metric(self, project_id: str, dashboard_metric_id: str) -> None:
        ...

    @abstractmethod
    def list_metrics(self, project_id: str) -> list[DashboardMetric]:
        ...
