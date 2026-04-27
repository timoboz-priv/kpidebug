import pytest

from kpidebug.metrics.computation import ComputationError, evaluate, validate


def _make_rows() -> list[dict]:
    return [
        {"revenue": 100.0, "transactions": 10.0, "status": "paid"},
        {"revenue": 200.0, "transactions": 5.0, "status": "paid"},
        {"revenue": 50.0, "status": "refunded"},
    ]


class TestValidation:
    def test_valid_simple_expression(self):
        validate("sum(revenue)")

    def test_valid_arithmetic(self):
        validate("sum(revenue) / count()")

    def test_valid_ratio(self):
        validate('ratio(sum(revenue), sum(transactions))')

    def test_valid_where_clause(self):
        validate('sum(revenue) where status = "paid"')

    def test_valid_where_with_and(self):
        validate('sum(revenue) where status = "paid" and revenue > "100"')

    def test_invalid_function(self):
        with pytest.raises(ComputationError):
            validate("exec(something)")

    def test_invalid_syntax(self):
        with pytest.raises(ComputationError):
            validate("sum(revenue")

    def test_unexpected_token(self):
        with pytest.raises(ComputationError):
            validate("sum(revenue) $$")


class TestEvaluate:
    def test_sum(self):
        rows = _make_rows()
        assert evaluate("sum(revenue)", rows) == 350.0

    def test_count(self):
        rows = _make_rows()
        assert evaluate("count()", rows) == 3.0

    def test_avg(self):
        rows = _make_rows()
        result = evaluate("avg(revenue)", rows)
        assert abs(result - 116.6666666) < 0.001

    def test_min(self):
        rows = _make_rows()
        assert evaluate("min(revenue)", rows) == 50.0

    def test_max(self):
        rows = _make_rows()
        assert evaluate("max(revenue)", rows) == 200.0

    def test_ratio(self):
        rows = _make_rows()
        result = evaluate("ratio(sum(revenue), sum(transactions))", rows)
        assert result == 350.0 / 15.0

    def test_arithmetic_expression(self):
        rows = _make_rows()
        result = evaluate("sum(revenue) / count()", rows)
        assert abs(result - 350.0 / 3.0) < 0.001

    def test_numeric_literal(self):
        rows = _make_rows()
        result = evaluate("sum(revenue) * 100 / sum(transactions)", rows)
        assert abs(result - 350.0 * 100 / 15.0) < 0.001

    def test_division_by_zero_returns_zero(self):
        rows = [{"a": 10.0}]
        result = evaluate("sum(a) / sum(b)", rows)
        assert result == 0.0

    def test_empty_records(self):
        result = evaluate("sum(revenue)", [])
        assert result == 0.0

    def test_avg_empty_returns_zero(self):
        result = evaluate("avg(revenue)", [])
        assert result == 0.0

    def test_where_clause(self):
        rows = _make_rows()
        result = evaluate('sum(revenue) where status = "paid"', rows)
        assert result == 300.0

    def test_where_with_numeric_comparison(self):
        rows = _make_rows()
        result = evaluate('count() where revenue > "100"', rows)
        assert result == 1.0

    def test_where_with_and(self):
        rows = _make_rows()
        result = evaluate('sum(revenue) where status = "paid" and revenue > "100"', rows)
        assert result == 200.0

    def test_ratio_with_where(self):
        rows = _make_rows()
        result = evaluate('ratio(count() where status = "paid", count())', rows)
        assert abs(result - 2.0 / 3.0) < 0.001

    def test_parenthesized_expression(self):
        rows = _make_rows()
        result = evaluate("(sum(revenue) + sum(transactions)) / count()", rows)
        assert abs(result - (350.0 + 15.0) / 3.0) < 0.001
