import uuid
from collections import defaultdict
from datetime import datetime, timezone

from kpidebug.data.types import DataRecord, DimensionValue
from kpidebug.metrics.types import MetricDefinition, MetricResult, MetricSource
from kpidebug.metrics.registry import registry
from kpidebug.metrics.computation import evaluate


class MetricEngine:
    def compute(self, definition: MetricDefinition, records: list[DataRecord]) -> list[MetricResult]:
        """Compute a metric against a set of data records.

        Filters records by source_filters, groups by dimensions,
        computes each group, and returns MetricResults.
        """
        filtered = self._filter_records(definition, records)
        groups = self._group_by_dimensions(definition.dimensions, filtered)

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        results: list[MetricResult] = []

        for dimension_values, group_records in groups.items():
            value = self._compute_value(definition, group_records, list(dimension_values))
            period_start, period_end = self._derive_period(group_records)

            result = MetricResult(
                id=str(uuid.uuid4()),
                metric_id=definition.id,
                project_id=definition.project_id,
                value=value,
                dimension_values=list(dimension_values),
                computed_at=now,
                period_start=period_start,
                period_end=period_end,
            )
            results.append(result)

        return results

    def _filter_records(
        self, definition: MetricDefinition, records: list[DataRecord]
    ) -> list[DataRecord]:
        if not definition.source_filters:
            return records

        filtered: list[DataRecord] = []
        for record in records:
            for sf in definition.source_filters:
                type_matches = record.source_type == sf.source_type
                field_matches = not sf.fields or record.field in sf.fields
                if type_matches and field_matches:
                    filtered.append(record)
                    break

        return filtered

    def _group_by_dimensions(
        self, dimensions: list[str], records: list[DataRecord]
    ) -> dict[tuple[DimensionValue, ...], list[DataRecord]]:
        if not dimensions:
            return {(): records}

        groups: dict[tuple[DimensionValue, ...], list[DataRecord]] = defaultdict(list)
        for record in records:
            key = self._extract_dimension_key(dimensions, record)
            groups[key].append(record)

        return dict(groups)

    def _extract_dimension_key(
        self, dimensions: list[str], record: DataRecord
    ) -> tuple[DimensionValue, ...]:
        values: list[DimensionValue] = []
        dim_map = {dv.dimension: dv for dv in record.dimension_values}
        for dim in dimensions:
            if dim in dim_map:
                values.append(dim_map[dim])
            else:
                values.append(DimensionValue(dimension=dim, value=""))
        return tuple(values)

    def _compute_value(
        self,
        definition: MetricDefinition,
        records: list[DataRecord],
        dimension_values: list[DimensionValue],
    ) -> float:
        if definition.source == MetricSource.BUILTIN:
            fn = registry.get(definition.builtin_key)
            if fn is None:
                raise ValueError(f"Unknown built-in metric: {definition.builtin_key}")
            return fn(records, dimension_values)
        else:
            if not definition.computation:
                raise ValueError(
                    f"AI-generated metric '{definition.name}' has no computation expression"
                )
            return evaluate(definition.computation, records)

    def _derive_period(self, records: list[DataRecord]) -> tuple[str, str]:
        timestamps = [r.timestamp for r in records if r.timestamp]
        if not timestamps:
            return ("", "")
        return (min(timestamps), max(timestamps))
