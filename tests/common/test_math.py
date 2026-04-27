from kpidebug.common.math import aggregate_values
from kpidebug.data.types import Aggregation


class TestAggregateValues:
    def test_sum(self):
        assert aggregate_values([10, 20, 30], Aggregation.SUM) == 60

    def test_avg(self):
        assert aggregate_values([10, 20, 30], Aggregation.AVG) == 20.0

    def test_avg_daily(self):
        assert aggregate_values([10, 20, 30], Aggregation.AVG_DAILY) == 20.0

    def test_min(self):
        assert aggregate_values([10, 20, 30], Aggregation.MIN) == 10

    def test_max(self):
        assert aggregate_values([10, 20, 30], Aggregation.MAX) == 30

    def test_count(self):
        assert aggregate_values([10, 20, 30], Aggregation.COUNT) == 3.0

    def test_empty_list(self):
        assert aggregate_values([], Aggregation.SUM) == 0.0
        assert aggregate_values([], Aggregation.AVG) == 0.0
        assert aggregate_values([], Aggregation.MIN) == 0.0

    def test_single_value(self):
        assert aggregate_values([42], Aggregation.SUM) == 42
        assert aggregate_values([42], Aggregation.AVG) == 42
        assert aggregate_values([42], Aggregation.MIN) == 42
        assert aggregate_values([42], Aggregation.MAX) == 42
