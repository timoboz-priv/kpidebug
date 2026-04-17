from unittest.mock import MagicMock

from kpidebug.data.data_store_firestore import FirestoreDataStore
from kpidebug.data.types import (
    DataSourceType,
    Dimension,
    DimensionType,
)


class TestFirestoreDataStore:
    def _make_store(self) -> tuple[FirestoreDataStore, MagicMock]:
        db = MagicMock()
        store = FirestoreDataStore(db)
        return store, db

    def test_create_source(self):
        store, db = self._make_store()
        dimensions = [
            Dimension(name="time", type=DimensionType.TEMPORAL),
            Dimension(name="geo.country", type=DimensionType.CATEGORICAL),
        ]
        source = store.create_source("p1", "Stripe Prod", DataSourceType.STRIPE, dimensions)
        assert source.id != ""
        assert source.name == "Stripe Prod"
        assert source.type == DataSourceType.STRIPE
        assert len(source.dimensions) == 2

    def test_create_source_with_credentials(self):
        store, db = self._make_store()
        source = store.create_source(
            "p1", "Stripe", DataSourceType.STRIPE, [],
            credentials={"api_key": "sk_test"},
        )
        assert source.credentials == {"api_key": "sk_test"}

    def test_get_source_returns_source(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = True
        doc.id = "s1"
        doc.to_dict.return_value = {
            "name": "Stripe Prod",
            "type": "stripe",
            "dimensions": [{"name": "time", "type": "temporal"}],
            "credentials": {"api_key": "sk_test"},
        }
        db.collection.return_value.document.return_value \
            .collection.return_value.document.return_value \
            .get.return_value = doc

        source = store.get_source("p1", "s1")
        assert source is not None
        assert source.id == "s1"
        assert source.type == DataSourceType.STRIPE
        assert source.credentials == {"api_key": "sk_test"}

    def test_get_source_returns_none(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value \
            .collection.return_value.document.return_value \
            .get.return_value = doc

        source = store.get_source("p1", "s1")
        assert source is None

    def test_delete_source(self):
        store, db = self._make_store()
        store.delete_source("p1", "s1")
        db.collection.return_value.document.return_value \
            .collection.return_value.document.return_value \
            .delete.assert_called_once()
