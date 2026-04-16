import json
from unittest.mock import MagicMock, call

from kpidebug.data.cache.firestore import FirestoreTableCache


class TestFirestoreTableCache:
    def _make_cache(
        self,
    ) -> tuple[FirestoreTableCache, MagicMock]:
        db = MagicMock()
        cache = FirestoreTableCache(db)
        return cache, db

    def test_is_cached_false_when_no_doc(self):
        cache, db = self._make_cache()
        doc = MagicMock()
        doc.exists = False
        (
            db.collection.return_value
            .document.return_value
            .get.return_value
        ) = doc
        assert not cache.is_cached("s1", "charges")

    def test_is_cached_true_when_marker_set(self):
        cache, db = self._make_cache()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {
            "_cached_charges": True
        }
        (
            db.collection.return_value
            .document.return_value
            .get.return_value
        ) = doc
        assert cache.is_cached("s1", "charges")

    def test_set_rows_stores_each_row(self):
        cache, db = self._make_cache()
        # Mock is_cached to return False for clear_table
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value.get.return_value = doc
        # Mock get for clear_table
        db.collection.return_value.document.return_value.collection.return_value.get.return_value = []

        rows = [
            {"id": "ch_1", "amount": 100},
            {"id": "ch_2", "amount": 200},
        ]
        cache.set_rows("s1", "charges", rows)

        # Should have called set for each row + marker
        ref = db.collection.return_value.document.return_value
        assert ref.collection.return_value.document.return_value.set.call_count >= 2

    def test_clear_table_deletes_docs(self):
        cache, db = self._make_cache()
        mock_doc = MagicMock()
        mock_doc.reference = MagicMock()
        db.collection.return_value.document.return_value.collection.return_value.get.return_value = [mock_doc]

        cache.clear_table("s1", "charges")

        mock_doc.reference.delete.assert_called_once()
