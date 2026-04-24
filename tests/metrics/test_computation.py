import pytest

from kpidebug.metrics.computation import ComputationError, evaluate, validate


def _make_rows() -> list[dict]:
    return [
        {"revenue": 100.0, "transactions": 10.0},
        {"revenue": 200.0, "transactions": 5.0},
        {"revenue": 50.0},
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
        rows = _make_rows()
        assert evaluate("sum('revenue')", rows) == 350.0

    def test_count(self):
        rows = _make_rows()
        assert evaluate("count('revenue')", rows) == 3.0

    def test_avg(self):
        rows = _make_rows()
        result = evaluate("avg('revenue')", rows)
        assert abs(result - 116.6666666) < 0.001

    def test_min_val(self):
        rows = _make_rows()
        assert evaluate("min_val('revenue')", rows) == 50.0

    def test_max_val(self):
        rows = _make_rows()
        assert evaluate("max_val('revenue')", rows) == 200.0

    def test_ratio(self):
        rows = _make_rows()
        assert evaluate("ratio('revenue', 'transactions')", rows) == 350.0 / 15.0

    def test_arithmetic_expression(self):
        rows = _make_rows()
        result = evaluate("sum('revenue') / count('transactions')", rows)
        assert result == 350.0 / 2.0

    def test_numeric_literal(self):
        rows = _make_rows()
        result = evaluate("sum('revenue') * 100 / sum('transactions')", rows)
        assert abs(result - 350.0 * 100 / 15.0) < 0.001

    def test_unary_negation(self):
        rows = _make_rows()
        result = evaluate("-sum('revenue')", rows)
        assert result == -350.0

    def test_division_by_zero_returns_zero(self):
        rows = [{"a": 10.0}]
        result = evaluate("sum('a') / sum('b')", rows)
        assert result == 0.0

    def test_empty_records(self):
        result = evaluate("sum('revenue')", [])
        assert result == 0.0

    def test_avg_empty_returns_zero(self):
        result = evaluate("avg('revenue')", [])
        assert result == 0.0
