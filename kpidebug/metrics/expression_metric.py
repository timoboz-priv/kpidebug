from kpidebug.data.types import Aggregation, TableFilter
from kpidebug.metrics.computation import evaluate_with_context
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.types import Metric, MetricResult, MetricDefinition


class ExpressionMetric(Metric):
    id: str
    name: str
    description: str
    data_type: str
    _definition: MetricDefinition

    def __init__(self, definition: MetricDefinition):
        self._definition = definition
        self.id = definition.id
        self.name = definition.name
        self.description = definition.description
        self.data_type = definition.data_type

    def compute_single(
        self,
        ctx: MetricContext,
        dimensions: list[str] | None = None,
        aggregation: Aggregation = Aggregation.SUM,
        filters: list[TableFilter] | None = None,
        days: int = 30,
        date: str | None = None,
    ) -> list[MetricResult]:
        value = evaluate_with_context(self._definition.computation, ctx)
        return [MetricResult(value=value)]
