from kpidebug.data.types import Aggregation


def aggregate_values(values: list[float], aggregation: Aggregation) -> float:
    if not values:
        return 0.0
    if aggregation == Aggregation.SUM:
        return sum(values)
    elif aggregation in (Aggregation.AVG, Aggregation.AVG_DAILY):
        return sum(values) / len(values)
    elif aggregation == Aggregation.MIN:
        return min(values)
    elif aggregation == Aggregation.MAX:
        return max(values)
    elif aggregation == Aggregation.COUNT:
        return float(len(values))
    return sum(values)
