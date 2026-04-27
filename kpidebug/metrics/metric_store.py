from abc import ABC, abstractmethod

from kpidebug.metrics.types import MetricDefinition, MetricDefinitionUpdate, StoredMetricResult


class AbstractMetricStore(ABC):
    @abstractmethod
    def create_definition(self, definition: MetricDefinition) -> MetricDefinition:
        ...

    @abstractmethod
    def get_definition(self, project_id: str, metric_id: str) -> MetricDefinition | None:
        ...

    @abstractmethod
    def list_definitions(self, project_id: str) -> list[MetricDefinition]:
        ...

    @abstractmethod
    def update_definition(
        self, project_id: str, metric_id: str, updates: MetricDefinitionUpdate,
    ) -> MetricDefinition:
        ...

    @abstractmethod
    def delete_definition(self, project_id: str, metric_id: str) -> None:
        ...

    @abstractmethod
    def store_results(self, results: list[StoredMetricResult]) -> None:
        ...

    @abstractmethod
    def get_results(
        self,
        project_id: str,
        metric_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[StoredMetricResult]:
        ...

    @abstractmethod
    def get_latest_result(self, project_id: str, metric_id: str) -> StoredMetricResult | None:
        ...
