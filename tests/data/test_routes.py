from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from kpidebug.api.auth import (
    get_current_user,
    get_data_source_store,
    get_project_store,
)
from kpidebug.api.server import app
from kpidebug.data.types import (
    DataSource,
    DataSourceType,
)
from kpidebug.management.types import ProjectMember, Role, User


def _mock_user() -> User:
    return User(
        id="user1", name="Alice",
        email="alice@test.com", avatar_url="",
    )


def _mock_project_store() -> MagicMock:
    store = MagicMock()
    store.get_member.return_value = ProjectMember(
        user_id="user1", role=Role.ADMIN,
        user_name="Alice", user_email="alice@test.com",
    )
    return store


def _mock_data_source_store() -> MagicMock:
    store = MagicMock()
    store.list_sources.return_value = [
        DataSource(
            id="s1", project_id="p1",
            name="Stripe Prod",
            type=DataSourceType.STRIPE,
        ),
    ]
    store.get_source.return_value = DataSource(
        id="s1", project_id="p1",
        name="Stripe Prod",
        type=DataSourceType.STRIPE,
    )
    store.create_source.return_value = DataSource(
        id="s1", project_id="p1",
        name="Stripe Prod",
        type=DataSourceType.STRIPE,
        credentials={"api_key": "sk_test_123"},
    )
    return store


class TestDataSourceRoutes:
    def setup_method(self):
        self.data_source_store = _mock_data_source_store()
        app.dependency_overrides[get_current_user] = lambda: _mock_user()
        app.dependency_overrides[get_data_source_store] = lambda: self.data_source_store
        app.dependency_overrides[get_project_store] = lambda: _mock_project_store()
        self.client = TestClient(app)

    def teardown_method(self):
        app.dependency_overrides.clear()

    def test_list_sources(self):
        response = self.client.get(
            "/api/projects/p1/data-sources",
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        assert len(response.json()) == 1

    @patch(
        "kpidebug.api.routes_data_sources"
        ".StripeConnector.validate_credentials"
    )
    def test_connect_source(self, mock_validate):
        mock_validate.return_value = True
        response = self.client.post(
            "/api/projects/p1/data-sources",
            json={
                "name": "Stripe Prod",
                "source_type": "stripe",
                "credentials": {"api_key": "sk_test_123"},
            },
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        self.data_source_store.create_source.assert_called_once()

    def test_disconnect_source(self):
        response = self.client.delete(
            "/api/projects/p1/data-sources/s1",
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200

    def test_list_tables(self):
        response = self.client.get(
            "/api/projects/p1/data-sources/s1/tables",
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        keys = [t["key"] for t in response.json()]
        assert "charges" in keys
        assert "customers" in keys
