from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from kpidebug.api.auth import FirebaseUser, get_current_user, get_user_store
from kpidebug.api.server import app
from kpidebug.management.types import User


def _mock_user() -> User:
    return User(id="user1", name="Alice", email="alice@test.com", avatar_url="")


def _mock_user_store() -> MagicMock:
    store = MagicMock()
    store.get.return_value = _mock_user()
    store.update.return_value = User(id="user1", name="Updated", email="alice@test.com", avatar_url="")
    return store


class TestUserRoutes:
    def setup_method(self):
        self.store = _mock_user_store()
        app.dependency_overrides[get_current_user] = lambda: _mock_user()
        app.dependency_overrides[get_user_store] = lambda: self.store
        self.client = TestClient(app)

    def teardown_method(self):
        app.dependency_overrides.clear()

    def test_get_me(self):
        response = self.client.get("/api/users/me")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "user1"
        assert data["name"] == "Alice"
        assert data["email"] == "alice@test.com"

    def test_update_me(self):
        response = self.client.put("/api/users/me", json={"name": "Updated"})
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated"

    def test_update_me_sends_all_fields(self):
        response = self.client.put("/api/users/me", json={"name": "Updated", "avatar_url": "http://img.test/a.png"})
        assert response.status_code == 200
        self.store.update.assert_called_once_with("user1", {"name": "Updated", "avatar_url": "http://img.test/a.png"})
