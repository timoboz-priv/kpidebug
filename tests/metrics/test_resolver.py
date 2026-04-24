from unittest.mock import MagicMock

from kpidebug.metrics.resolver import (
    is_builtin_id,
    list_builtins_for_tables,
    parse_builtin_key,
    resolve_builtin,
    resolve_metric,
)
from kpidebug.metrics.types import (
    MetricDataType,
    MetricDefinition,
    MetricSource,
)


class TestIsBuiltinId:
    def test_builtin_prefix(self):
        assert is_builtin_id("builtin:stripe.gross_revenue") is True

    def test_uuid(self):
        assert is_builtin_id("abc-123-def") is False

    def test_empty(self):
        assert is_builtin_id("") is False


class TestParseBuiltinKey:
    def test_extracts_key(self):
        assert parse_builtin_key("builtin:stripe.mrr") == "stripe.mrr"


class TestResolveBuiltin:
    def test_known_metric(self):
        resolved = resolve_builtin("builtin:stripe.gross_revenue", source_id="src1")
        assert resolved is not None
        assert resolved.id == "builtin:stripe.gross_revenue"
        assert resolved.name == "Gross Revenue"
        assert resolved.source == MetricSource.BUILTIN
        assert resolved.source_id == "src1"
        assert resolved.builtin is not None

    def test_unknown_metric(self):
        assert resolve_builtin("builtin:nonexistent") is None


class TestResolveMetric:
    def test_resolves_builtin(self):
        metric_store = MagicMock()
        resolved = resolve_metric("builtin:stripe.mrr", "p1", metric_store)
        assert resolved is not None
        assert resolved.source == MetricSource.BUILTIN
        metric_store.get_definition.assert_not_called()

    def test_resolves_definition(self):
        metric_store = MagicMock()
        metric_store.get_definition.return_value = MetricDefinition(
            id="m1", project_id="p1", name="Custom",
            data_type=MetricDataType.NUMBER,
            source=MetricSource.EXPRESSION,
            source_id="src1", table="charges",
            computation="sum('amount')",
        )
        resolved = resolve_metric("m1", "p1", metric_store)
        assert resolved is not None
        assert resolved.source == MetricSource.EXPRESSION
        assert resolved.definition is not None

    def test_returns_none_for_unknown(self):
        metric_store = MagicMock()
        metric_store.get_definition.return_value = None
        assert resolve_metric("nonexistent", "p1", metric_store) is None


class TestListBuiltinsForTables:
    def test_filters_by_table(self):
        results = list_builtins_for_tables("src1", {"charges"})
        keys = [r.id for r in results]
        assert "builtin:stripe.gross_revenue" in keys
        assert "builtin:stripe.refund_rate" in keys
        assert "builtin:stripe.net_revenue" not in keys

    def test_empty_tables(self):
        assert list_builtins_for_tables("src1", set()) == []
