from abc import ABC, abstractmethod

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.types import AnalysisResult


class Analyzer(ABC):
    @abstractmethod
    def analyze(self, ctx: AnalysisContext) -> AnalysisResult:
        ...
