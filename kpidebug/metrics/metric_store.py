from abc import ABC, abstractmethod

from kpidebug.metrics.types import MetricDefinition, MetricDefinitionUpdate, MetricResult


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
    def list_for_source(self, project_id: str, source_id: str) -> list[MetricDefinition]:
        ...

    @abstractmethod
    def get_by_builtin_key(
        self, project_id: str, source_id: str, builtin_key: str,
    ) -> MetricDefinition | None:
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
    def store_results(self, results: list[MetricResult]) -> None:
        ...

    @abstractmethod
    def get_results(
        self,
        project_id: str,
        metric_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[MetricResult]:
        ...

    @abstractmethod
    def get_latest_result(self, project_id: str, metric_id: str) -> MetricResult | None:
        ...
