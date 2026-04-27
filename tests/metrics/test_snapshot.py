from kpidebug.metrics.types import MetricSnapshot


class TestMetricSnapshotChange:
    def test_change_basic(self):
        snapshot = MetricSnapshot(values=[3, 2, 4, 2])
        result = snapshot.change(2)
        assert abs(result - 0.2) < 0.001

    def test_change_y1(self):
        snapshot = MetricSnapshot(values=[10, 20])
        result = snapshot.change(1)
        assert result == 1.0

    def test_change_negative(self):
        snapshot = MetricSnapshot(values=[20, 10])
        result = snapshot.change(1)
        assert result == -0.5

    def test_change_zero_previous(self):
        snapshot = MetricSnapshot(values=[0, 10])
        result = snapshot.change(1)
        assert result == 0.0

    def test_change_insufficient_data(self):
        snapshot = MetricSnapshot(values=[10])
        result = snapshot.change(1)
        assert result == 0.0

    def test_change_empty_values(self):
        snapshot = MetricSnapshot(values=[])
        result = snapshot.change(1)
        assert result == 0.0

    def test_change_y3(self):
        snapshot = MetricSnapshot(values=[1, 2, 3, 4, 5, 6])
        result = snapshot.change(3)
        avg_recent = (4 + 5 + 6) / 3
        avg_previous = (1 + 2 + 3) / 3
        expected = (avg_recent - avg_previous) / avg_previous
        assert abs(result - expected) < 0.001

    def test_change_all_same(self):
        snapshot = MetricSnapshot(values=[5, 5, 5, 5])
        result = snapshot.change(2)
        assert result == 0.0
