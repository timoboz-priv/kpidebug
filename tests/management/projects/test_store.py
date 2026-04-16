from unittest.mock import MagicMock, patch

from kpidebug.management.types import Role, Project
from kpidebug.management.project_store_firestore import FirestoreProjectStore


class TestFirestoreProjectStore:
    def _make_store(self) -> tuple[FirestoreProjectStore, MagicMock]:
        db = MagicMock()
        store = FirestoreProjectStore(db)
        return store, db

    def test_get_returns_project_when_exists(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = True
        doc.id = "proj1"
        doc.to_dict.return_value = {"name": "My Project", "description": "A test project"}
        db.collection.return_value.document.return_value.get.return_value = doc

        project = store.get("proj1")

        assert project is not None
        assert project.id == "proj1"
        assert project.name == "My Project"
        assert project.description == "A test project"

    def test_get_returns_none_when_not_exists(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value.get.return_value = doc

        project = store.get("proj1")

        assert project is None

    @patch("kpidebug.management.project_store_firestore.uuid.uuid4")
    def test_create_project_with_admin_membership(self, mock_uuid):
        mock_uuid.return_value = "test-uuid-1234"
        store, db = self._make_store()

        project = store.create(
            name="New Project",
            description="Desc",
            creator_id="user1",
            creator_name="Alice",
            creator_email="alice@test.com",
        )

        assert project.id == "test-uuid-1234"
        assert project.name == "New Project"
        # Should have set the project doc
        db.collection.return_value.document.return_value.set.assert_called()

    def test_get_member_returns_member(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {
            "user_id": "user1",
            "role": "admin",
            "user_name": "Alice",
            "user_email": "alice@test.com",
        }
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value = doc

        member = store.get_member("proj1", "user1")

        assert member is not None
        assert member.user_id == "user1"
        assert member.role == Role.ADMIN

    def test_get_member_returns_none_when_not_found(self):
        store, db = self._make_store()
        doc = MagicMock()
        doc.exists = False
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.get.return_value = doc

        member = store.get_member("proj1", "user1")

        assert member is None

    def test_add_member(self):
        store, db = self._make_store()

        member = store.add_member("proj1", "user1", Role.EDIT, "Alice", "alice@test.com")

        assert member.user_id == "user1"
        assert member.role == Role.EDIT
        db.collection.return_value.document.return_value.collection.return_value.document.return_value.set.assert_called_once()

    def test_remove_member(self):
        store, db = self._make_store()

        store.remove_member("proj1", "user1")

        db.collection.return_value.document.return_value.collection.return_value.document.return_value.delete.assert_called_once()

    def test_delete_project(self):
        store, db = self._make_store()
        # Mock get_members to return empty list
        db.collection.return_value.document.return_value.collection.return_value.get.return_value = []

        store.delete("proj1")

        db.collection.return_value.document.return_value.delete.assert_called_once()
