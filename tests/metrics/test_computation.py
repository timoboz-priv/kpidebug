import pytest

from kpidebug.data.types import DataSourceType
from kpidebug.metrics.types import DataRecord
from kpidebug.metrics.computation import ComputationError, evaluate, validate


def _make_records() -> list[DataRecord]:
    return [
        DataRecord(field="revenue", value=100.0, source_type=DataSourceType.STRIPE),
        DataRecord(field="revenue", value=200.0, source_type=DataSourceType.STRIPE),
        DataRecord(field="revenue", value=50.0, source_type=DataSourceType.STRIPE),
        DataRecord(field="transactions", value=10.0, source_type=DataSourceType.STRIPE),
        DataRecord(field="transactions", value=5.0, source_type=DataSourceType.STRIPE),
    ]


class TestValidation:
    def test_valid_simple_expression(self):
        validate("sum('revenue')")

    def test_valid_arithmetic(self):
        validate("sum('revenue') / count('transactions')")

    def test_valid_ratio(self):
        validate("ratio('revenue', 'transactions')")

    def test_invalid_function(self):
        with pytest.raises(ComputationError, match="Unknown function"):
            validate("exec('os.system')")

    def test_invalid_syntax(self):
        with pytest.raises(ComputationError, match="Invalid expression syntax"):
            validate("sum('revenue'")

    def test_non_string_argument(self):
        with pytest.raises(ComputationError, match="must be string literals"):
            validate("sum(42)")

    def test_wrong_arg_count(self):
        with pytest.raises(ComputationError, match="expects 1 argument"):
            validate("sum('a', 'b')")

    def test_attribute_access_blocked(self):
        with pytest.raises(ComputationError, match="Only direct function calls are allowed"):
            validate("__import__('os').system('ls')")


class TestEvaluate:
    def test_sum(self):
        records = _make_records()
        assert evaluate("sum('revenue')", records) == 350.0

    def test_count(self):
        records = _make_records()
        assert evaluate("count('revenue')", records) == 3.0

    def test_avg(self):
        records = _make_records()
        result = evaluate("avg('revenue')", records)
        assert abs(result - 116.6666666) < 0.001

    def test_min_val(self):
        records = _make_records()
        assert evaluate("min_val('revenue')", records) == 50.0

    def test_max_val(self):
        records = _make_records()
        assert evaluate("max_val('revenue')", records) == 200.0

    def test_ratio(self):
        records = _make_records()
        assert evaluate("ratio('revenue', 'transactions')", records) == 350.0 / 15.0

    def test_arithmetic_expression(self):
        records = _make_records()
        result = evaluate("sum('revenue') / count('transactions')", records)
        assert result == 350.0 / 2.0

    def test_numeric_literal(self):
        records = _make_records()
        result = evaluate("sum('revenue') * 100 / sum('transactions')", records)
        assert abs(result - 350.0 * 100 / 15.0) < 0.001

    def test_unary_negation(self):
        records = _make_records()
        result = evaluate("-sum('revenue')", records)
        assert result == -350.0

    def test_division_by_zero_returns_zero(self):
        records = [DataRecord(field="a", value=10.0)]
        result = evaluate("sum('a') / sum('b')", records)
        assert result == 0.0

    def test_empty_records(self):
        result = evaluate("sum('revenue')", [])
        assert result == 0.0

    def test_avg_empty_returns_zero(self):
        result = evaluate("avg('revenue')", [])
        assert result == 0.0
