from unittest.mock import MagicMock

from kpidebug.management.user_store_firestore import FirestoreUserStore
from kpidebug.management.types import User


class TestFirestoreUserStore:
    def _make_store(self) -> tuple[FirestoreUserStore, MagicMock]:
        db = MagicMock()
        store = FirestoreUserStore(db)
        return store, db

    def _mock_doc(self, db: MagicMock, exists: bool, data: dict | None = None, doc_id: str = "user1"):
        doc = MagicMock()
        doc.exists = exists
        doc.id = doc_id
        doc.to_dict.return_value = data or {}
        db.collection.return_value.document.return_value.get.return_value = doc
        return doc

    def test_get_returns_user_when_exists(self):
        store, db = self._make_store()
        self._mock_doc(db, True, {"name": "Alice", "email": "alice@test.com", "avatar_url": ""})

        user = store.get("user1")

        assert user is not None
        assert user.id == "user1"
        assert user.name == "Alice"
        assert user.email == "alice@test.com"

    def test_get_returns_none_when_not_exists(self):
        store, db = self._make_store()
        self._mock_doc(db, False)

        user = store.get("user1")

        assert user is None

    def test_create_writes_to_firestore(self):
        store, db = self._make_store()
        user = User(id="user1", name="Alice", email="alice@test.com", avatar_url="")

        result = store.create(user)

        db.collection.return_value.document.return_value.set.assert_called_once_with({
            "name": "Alice",
            "email": "alice@test.com",
            "avatar_url": "",
        })
        assert result.id == "user1"

    def test_update_calls_firestore_update(self):
        store, db = self._make_store()
        self._mock_doc(db, True, {"name": "Bob", "email": "bob@test.com", "avatar_url": ""})

        result = store.update("user1", {"name": "Bob"})

        db.collection.return_value.document.return_value.update.assert_called_once_with({"name": "Bob"})
        assert result.name == "Bob"

    def test_get_or_create_returns_existing(self):
        store, db = self._make_store()
        self._mock_doc(db, True, {"name": "Alice", "email": "alice@test.com", "avatar_url": ""})

        user = store.get_or_create("user1", "alice@test.com", "Alice", None)

        assert user.name == "Alice"
        db.collection.return_value.document.return_value.set.assert_not_called()

    def test_get_or_create_creates_when_not_exists(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = False
        doc.id = "user1"

        # First call to get returns not found, second call after create returns the user
        get_mock = db.collection.return_value.document.return_value.get
        get_mock.return_value = doc

        user = store.get_or_create("user1", "alice@test.com", "Alice", None)

        assert user.id == "user1"
        assert user.name == "Alice"
        assert user.email == "alice@test.com"
        db.collection.return_value.document.return_value.set.assert_called_once()
