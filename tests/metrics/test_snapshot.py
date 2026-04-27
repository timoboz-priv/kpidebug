from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import MetricSnapshot


class TestAggregateValue:
    def test_sum_7d(self):
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6, 7])
        assert snap.aggregate_value(7, Aggregation.SUM) == 28

    def test_sum_3d(self):
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6, 7])
        assert snap.aggregate_value(3, Aggregation.SUM) == 18

    def test_sum_1d(self):
        snap = MetricSnapshot(values=[1, 2, 3])
        assert snap.aggregate_value(1, Aggregation.SUM) == 3

    def test_avg_daily_7d(self):
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6, 7])
        assert snap.aggregate_value(7, Aggregation.AVG_DAILY) == 4.0

    def test_avg_daily_3d(self):
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6, 7])
        assert snap.aggregate_value(3, Aggregation.AVG_DAILY) == 6.0

    def test_avg_daily_1d(self):
        snap = MetricSnapshot(values=[10, 20])
        assert snap.aggregate_value(1, Aggregation.AVG_DAILY) == 20.0

    def test_default_is_sum(self):
        snap = MetricSnapshot(values=[10, 20, 30])
        assert snap.aggregate_value(3) == 60

    def test_empty(self):
        snap = MetricSnapshot(values=[])
        assert snap.aggregate_value(7, Aggregation.SUM) == 0.0

    def test_fewer_values_than_days(self):
        snap = MetricSnapshot(values=[5, 10])
        assert snap.aggregate_value(7, Aggregation.SUM) == 15


class TestChangeSumAggregation:
    def test_change_1d_sum(self):
        # last 1 day: 20, previous 1 day: 10 => (20-10)/10 = 1.0
        snap = MetricSnapshot(values=[10, 20])
        assert snap.change(1, Aggregation.SUM) == 1.0

    def test_change_3d_sum(self):
        # last 3: [4,5,6] sum=15, prev 3: [1,2,3] sum=6 => (15-6)/6 = 1.5
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6])
        assert abs(snap.change(3, Aggregation.SUM) - 1.5) < 0.001

    def test_change_7d_sum(self):
        # 14 values: first 7 sum=28, last 7 sum=77 => (77-28)/28
        vals = list(range(1, 15))
        snap = MetricSnapshot(values=vals)
        recent = sum(vals[-7:])
        prev = sum(vals[-14:-7])
        expected = (recent - prev) / prev
        assert abs(snap.change(7, Aggregation.SUM) - expected) < 0.001

    def test_change_30d_sum(self):
        vals = [float(i) for i in range(60)]
        snap = MetricSnapshot(values=vals)
        recent = sum(vals[-30:])
        prev = sum(vals[-60:-30])
        expected = (recent - prev) / prev
        assert abs(snap.change(30, Aggregation.SUM) - expected) < 0.001

    def test_change_negative(self):
        snap = MetricSnapshot(values=[20, 10])
        assert snap.change(1, Aggregation.SUM) == -0.5

    def test_change_zero_previous(self):
        snap = MetricSnapshot(values=[0, 10])
        assert snap.change(1, Aggregation.SUM) == 0.0

    def test_change_insufficient_data(self):
        snap = MetricSnapshot(values=[10])
        assert snap.change(1, Aggregation.SUM) == 0.0

    def test_change_default_is_sum(self):
        snap = MetricSnapshot(values=[10, 20])
        assert snap.change(1) == 1.0


class TestChangeAvgDailyAggregation:
    def test_change_1d_avg(self):
        # avg of last 1: 20, avg of prev 1: 10 => (20-10)/10 = 1.0
        snap = MetricSnapshot(values=[10, 20])
        assert snap.change(1, Aggregation.AVG_DAILY) == 1.0

    def test_change_3d_avg(self):
        # avg of last 3: (4+5+6)/3=5, avg of prev 3: (1+2+3)/3=2 => (5-2)/2 = 1.5
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6])
        assert abs(snap.change(3, Aggregation.AVG_DAILY) - 1.5) < 0.001

    def test_change_7d_avg_daily(self):
        # Same ratio as sum since avg = sum/n and n is equal on both sides
        vals = list(range(1, 15))
        snap = MetricSnapshot(values=[float(v) for v in vals])
        sum_change = snap.change(7, Aggregation.SUM)
        avg_change = snap.change(7, Aggregation.AVG_DAILY)
        assert abs(sum_change - avg_change) < 0.001

    def test_change_30d_avg_daily(self):
        vals = [float(i) for i in range(60)]
        snap = MetricSnapshot(values=vals)
        recent_avg = sum(vals[-30:]) / 30
        prev_avg = sum(vals[-60:-30]) / 30
        expected = (recent_avg - prev_avg) / prev_avg
        assert abs(snap.change(30, Aggregation.AVG_DAILY) - expected) < 0.001

    def test_bounce_rate_scenario(self):
        # Bounce rate: [0.7, 0.8, 0.0, 0.5, 0.6, 0.0]
        # 3d change: avg of [0.5,0.6,0.0]=0.367, avg of [0.7,0.8,0.0]=0.5
        # => (0.367-0.5)/0.5 = -0.267
        snap = MetricSnapshot(values=[0.7, 0.8, 0.0, 0.5, 0.6, 0.0])
        recent_avg = (0.5 + 0.6 + 0.0) / 3
        prev_avg = (0.7 + 0.8 + 0.0) / 3
        expected = (recent_avg - prev_avg) / abs(prev_avg)
        assert abs(snap.change(3, Aggregation.AVG_DAILY) - expected) < 0.001


class TestChangeMinMaxAggregation:
    def test_change_min(self):
        # min of last 3: 4, min of prev 3: 1 => (4-1)/1 = 3.0
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6])
        assert snap.change(3, Aggregation.MIN) == 3.0

    def test_change_max(self):
        # max of last 3: 6, max of prev 3: 3 => (6-3)/3 = 1.0
        snap = MetricSnapshot(values=[1, 2, 3, 4, 5, 6])
        assert snap.change(3, Aggregation.MAX) == 1.0
