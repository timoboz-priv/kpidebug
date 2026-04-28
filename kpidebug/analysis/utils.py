from enum import Enum


class ChangeCategory(str, Enum):
    LARGE_DROP = "large_drop"
    SMALL_DROP = "small_drop"
    NEGLIGIBLE = "negligible"
    SMALL_GAIN = "small_gain"
    LARGE_GAIN = "large_gain"


SMALL_DROP_THRESHOLD = -0.05
LARGE_DROP_THRESHOLD = -0.15
SMALL_GAIN_THRESHOLD = 0.05
LARGE_GAIN_THRESHOLD = 0.15
NEGLIGIBLE_THRESHOLD = 0.02

MIN_DATA_POINTS = 7
TREND_WINDOW_DAYS = 7
COMPARISON_WINDOW_DAYS = 30


def classify_change(change: float) -> ChangeCategory:
    if change <= LARGE_DROP_THRESHOLD:
        return ChangeCategory.LARGE_DROP
    if change <= SMALL_DROP_THRESHOLD:
        return ChangeCategory.SMALL_DROP
    if change >= LARGE_GAIN_THRESHOLD:
        return ChangeCategory.LARGE_GAIN
    if change >= SMALL_GAIN_THRESHOLD:
        return ChangeCategory.SMALL_GAIN
    return ChangeCategory.NEGLIGIBLE
