from kpidebug.metrics.builtin_metrics import MetricComputeResult, builtin_registry
from kpidebug.metrics.computation import evaluate
from kpidebug.metrics.filters import apply_filters
from kpidebug.metrics.resolver import ResolvedMetric
from kpidebug.metrics.types import MetricComputeInput, MetricSource


def compute_metric(
    resolved: ResolvedMetric,
    compute_input: MetricComputeInput,
) -> list[MetricComputeResult]:
    if resolved.source == MetricSource.BUILTIN:
        return _compute_builtin(resolved, compute_input)
    elif resolved.source == MetricSource.EXPRESSION:
        return _compute_expression(resolved, compute_input)
    raise ValueError(f"Unknown metric source: {resolved.source}")


def _compute_builtin(
    resolved: ResolvedMetric,
    compute_input: MetricComputeInput,
) -> list[MetricComputeResult]:
    if resolved.builtin is None:
        raise ValueError(f"Builtin metric not found: {resolved.id}")

    return builtin_registry.compute(
        resolved.builtin.key, compute_input,
    )


def _compute_expression(
    resolved: ResolvedMetric,
    compute_input: MetricComputeInput,
) -> list[MetricComputeResult]:
    if resolved.definition is None or not resolved.definition.computation:
        raise ValueError(f"Expression metric has no computation: {resolved.id}")

    rows = apply_filters(compute_input.rows, compute_input.filters)
    value = evaluate(resolved.definition.computation, rows)
    return [MetricComputeResult(value=value)]
