from unittest.mock import MagicMock

from kpidebug.metrics.metric_store_firestore import FirestoreMetricStore
from kpidebug.metrics.types import (
    MetricDataType,
    MetricDefinition,
    MetricResult,
    MetricSource,
    SourceFilter,
)
from kpidebug.data.types import DataSourceType, DimensionValue


class TestFirestoreMetricStore:
    def _make_store(self) -> tuple[FirestoreMetricStore, MagicMock]:
        db = MagicMock()
        store = FirestoreMetricStore(db)
        return store, db

    def test_create_definition(self):
        store, db = self._make_store()

        definition = MetricDefinition(
            project_id="p1",
            name="Revenue",
            description="Total revenue",
            data_type=MetricDataType.CURRENCY,
            source=MetricSource.AI_GENERATED,
            computation="sum('revenue')",
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE, fields=["revenue"])],
            dimensions=["geo.country"],
        )

        result = store.create_definition(definition)

        assert result.id != ""
        assert result.name == "Revenue"
        assert result.created_at != ""
        assert result.updated_at != ""
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.set.assert_called_once()

    def test_get_definition_returns_definition(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = True
        doc.id = "m1"
        doc.to_dict.return_value = {
            "name": "Revenue",
            "description": "Total revenue",
            "data_type": "currency",
            "source": "ai_generated",
            "builtin_key": "",
            "computation": "sum('revenue')",
            "source_filters": [{"source_type": "stripe", "fields": ["revenue"]}],
            "dimensions": ["geo.country"],
            "created_at": "2026-04-01T00:00:00Z",
            "updated_at": "2026-04-01T00:00:00Z",
        }
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value = doc

        result = store.get_definition("p1", "m1")

        assert result is not None
        assert result.id == "m1"
        assert result.name == "Revenue"
        assert result.data_type == MetricDataType.CURRENCY
        assert result.source == MetricSource.AI_GENERATED
        assert len(result.source_filters) == 1
        assert result.source_filters[0].source_type == DataSourceType.STRIPE

    def test_get_definition_returns_none(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value = doc

        result = store.get_definition("p1", "m1")

        assert result is None

    def test_delete_definition(self):
        store, db = self._make_store()

        store.delete_definition("p1", "m1")

        db.collection.return_value.document.return_value.collection.return_value.document.return_value.delete.assert_called_once()

    def test_store_results(self):
        store, db = self._make_store()
        results = [
            MetricResult(
                id="r1", metric_id="m1", project_id="p1",
                value=100.0,
                dimension_values=[DimensionValue(dimension="geo.country", value="US")],
                computed_at="2026-04-01T00:00:00Z",
                period_start="2026-03-01T00:00:00Z",
                period_end="2026-03-31T00:00:00Z",
            ),
        ]

        store.store_results(results)

        db.batch.return_value.set.assert_called_once()
        db.batch.return_value.commit.assert_called_once()

    def test_store_results_empty_list(self):
        store, db = self._make_store()

        store.store_results([])

        db.batch.assert_not_called()
