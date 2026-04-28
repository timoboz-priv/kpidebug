from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone

from kpidebug.analysis.analyzer import Analyzer
from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.types import AnalysisResult, Insight


class InsightTemplate(ABC):
    @abstractmethod
    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        ...


class TemplateAnalyzer(Analyzer):
    _templates: list[InsightTemplate]

    def __init__(self, templates: list[InsightTemplate]):
        self._templates = templates

    def analyze(self, ctx: AnalysisContext) -> AnalysisResult:
        insights: list[Insight] = []
        for template in self._templates:
            insight = template.evaluate(ctx)
            if insight is not None:
                insights.append(insight)
        return AnalysisResult(
            insights=insights,
            analyzed_at=datetime.now(timezone.utc),
        )
