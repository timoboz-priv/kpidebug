from datetime import datetime, timezone
from unittest.mock import MagicMock

from kpidebug.analysis.analyzer_template import InsightTemplate, TemplateAnalyzer
from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.types import Insight, Signal


class _AlwaysFiresTemplate(InsightTemplate):
    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        return Insight(headline="Test insight")


class _NeverFiresTemplate(InsightTemplate):
    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        return None


class TestTemplateAnalyzer:
    def test_collects_insights_from_templates(self):
        analyzer = TemplateAnalyzer([_AlwaysFiresTemplate(), _AlwaysFiresTemplate()])
        ctx = MagicMock(spec=AnalysisContext)

        result = analyzer.analyze(ctx)

        assert len(result.insights) == 2
        assert result.insights[0].headline == "Test insight"

    def test_skips_none_results(self):
        analyzer = TemplateAnalyzer([_NeverFiresTemplate(), _AlwaysFiresTemplate(), _NeverFiresTemplate()])
        ctx = MagicMock(spec=AnalysisContext)

        result = analyzer.analyze(ctx)

        assert len(result.insights) == 1

    def test_empty_templates_returns_empty_result(self):
        analyzer = TemplateAnalyzer([])
        ctx = MagicMock(spec=AnalysisContext)

        result = analyzer.analyze(ctx)

        assert result.insights == []
        assert isinstance(result.analyzed_at, datetime)
