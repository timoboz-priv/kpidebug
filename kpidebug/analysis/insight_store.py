from abc import ABC, abstractmethod

from kpidebug.analysis.types import Insight


class AbstractInsightStore(ABC):
    @abstractmethod
    def store_insights(self, project_id: str, insights: list[Insight]) -> None:
        ...

    @abstractmethod
    def list_insights(self, project_id: str, limit: int = 20) -> list[Insight]:
        ...
