from abc import ABC, abstractmethod

from kpidebug.data.types import DataSource, DataSourceType, Dimension


class AbstractDataStore(ABC):
    @abstractmethod
    def create_source(
        self, project_id: str, name: str,
        source_type: DataSourceType, dimensions: list[Dimension],
    ) -> DataSource:
        ...

    @abstractmethod
    def get_source(self, project_id: str, source_id: str) -> DataSource | None:
        ...

    @abstractmethod
    def list_sources(self, project_id: str) -> list[DataSource]:
        ...

    @abstractmethod
    def delete_source(self, project_id: str, source_id: str) -> None:
        ...

    @abstractmethod
    def store_credentials(
        self, project_id: str, source_id: str,
        credentials: dict[str, str],
    ) -> None:
        ...

    @abstractmethod
    def get_credentials(
        self, project_id: str, source_id: str,
    ) -> dict[str, str] | None:
        ...

    @abstractmethod
    def delete_credentials(
        self, project_id: str, source_id: str,
    ) -> None:
        ...
