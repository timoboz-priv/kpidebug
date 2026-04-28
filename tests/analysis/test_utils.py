from kpidebug.analysis.utils import ChangeCategory, classify_change


class TestClassifyChange:
    def test_large_drop(self):
        assert classify_change(-0.20) == ChangeCategory.LARGE_DROP
        assert classify_change(-0.15) == ChangeCategory.LARGE_DROP

    def test_small_drop(self):
        assert classify_change(-0.10) == ChangeCategory.SMALL_DROP
        assert classify_change(-0.05) == ChangeCategory.SMALL_DROP

    def test_negligible(self):
        assert classify_change(0.0) == ChangeCategory.NEGLIGIBLE
        assert classify_change(0.01) == ChangeCategory.NEGLIGIBLE
        assert classify_change(-0.01) == ChangeCategory.NEGLIGIBLE

    def test_small_gain(self):
        assert classify_change(0.05) == ChangeCategory.SMALL_GAIN
        assert classify_change(0.10) == ChangeCategory.SMALL_GAIN

    def test_large_gain(self):
        assert classify_change(0.15) == ChangeCategory.LARGE_GAIN
        assert classify_change(0.50) == ChangeCategory.LARGE_GAIN

    def test_boundary_at_negligible(self):
        assert classify_change(0.02) == ChangeCategory.NEGLIGIBLE
        assert classify_change(-0.02) == ChangeCategory.NEGLIGIBLE
        assert classify_change(0.03) == ChangeCategory.NEGLIGIBLE
        assert classify_change(-0.04) == ChangeCategory.NEGLIGIBLE
