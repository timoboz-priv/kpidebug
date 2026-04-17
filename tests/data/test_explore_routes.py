from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from kpidebug.api.auth import (
    get_current_user,
    get_data_store,
    get_project_store,
)
from kpidebug.api.server import app
from kpidebug.data.types import (
    DataRecord,
    DataSourceType,
    DataSource,
    DimensionValue,
)
from kpidebug.management.types import ProjectMember, Role, User


def _mock_user() -> User:
    return User(id="user1", name="Alice", email="alice@test.com", avatar_url="")


def _mock_project_store() -> MagicMock:
    store = MagicMock()
    store.get_member.return_value = ProjectMember(
        user_id="user1", role=Role.ADMIN,
        user_name="Alice", user_email="alice@test.com",
    )
    return store


def _make_records() -> list[DataRecord]:
    return [
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="charges.amount", value=100.0,
            timestamp="2026-04-01T00:00:00Z",
            dimension_values=[
                DimensionValue(dimension="currency", value="usd"),
                DimensionValue(dimension="charge_status", value="succeeded"),
            ],
        ),
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="charges.amount", value=200.0,
            timestamp="2026-04-02T00:00:00Z",
            dimension_values=[
                DimensionValue(dimension="currency", value="eur"),
                DimensionValue(dimension="charge_status", value="succeeded"),
            ],
        ),
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="charges.amount", value=50.0,
            timestamp="2026-04-03T00:00:00Z",
            dimension_values=[
                DimensionValue(dimension="currency", value="usd"),
                DimensionValue(dimension="charge_status", value="failed"),
            ],
        ),
    ]


class TestMetricExploreEndpoint:
    def setup_method(self):
        self.data_store = MagicMock()
        self.data_store.get_source.return_value = DataSource(
            id="s1", project_id="p1", name="Stripe",
            type=DataSourceType.STRIPE,
            credentials={"api_key": "sk_test"},
        )
        app.dependency_overrides[get_current_user] = lambda: _mock_user()
        app.dependency_overrides[get_data_store] = lambda: self.data_store
        app.dependency_overrides[get_project_store] = lambda: _mock_project_store()
        self.client = TestClient(app)

    def teardown_method(self):
        app.dependency_overrides.clear()

    @patch("kpidebug.api.routes_metric_explore.CONNECTORS")
    def test_sum_aggregation(self, mock_connectors):
        mock_connector = MagicMock()
        mock_connector.fetch_metric_data.return_value = _make_records()
        mock_connectors.get.return_value = mock_connector

        response = self.client.post(
            "/api/projects/p1/metrics/explore",
            json={
                "source_id": "s1",
                "metric_key": "charges.amount",
                "aggregation": "sum",
            },
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["metric_key"] == "charges.amount"
        assert data["results"][0]["value"] == 350.0
        assert data["record_count"] == 3

    @patch("kpidebug.api.routes_metric_explore.CONNECTORS")
    def test_group_by_dimension(self, mock_connectors):
        mock_connector = MagicMock()
        mock_connector.fetch_metric_data.return_value = _make_records()
        mock_connectors.get.return_value = mock_connector

        response = self.client.post(
            "/api/projects/p1/metrics/explore",
            json={
                "source_id": "s1",
                "metric_key": "charges.amount",
                "aggregation": "sum",
                "group_by": "currency",
            },
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        results = {r["group"]: r["value"] for r in response.json()["results"]}
        assert results["usd"] == 150.0
        assert results["eur"] == 200.0

    @patch("kpidebug.api.routes_metric_explore.CONNECTORS")
    def test_dimension_filter(self, mock_connectors):
        mock_connector = MagicMock()
        mock_connector.fetch_metric_data.return_value = _make_records()
        mock_connectors.get.return_value = mock_connector

        response = self.client.post(
            "/api/projects/p1/metrics/explore",
            json={
                "source_id": "s1",
                "metric_key": "charges.amount",
                "aggregation": "sum",
                "filters": [{"dimension": "charge_status", "value": "succeeded"}],
            },
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 200
        assert response.json()["results"][0]["value"] == 300.0

    def test_missing_metric_key(self):
        response = self.client.post(
            "/api/projects/p1/metrics/explore",
            json={"source_id": "s1", "aggregation": "sum"},
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 400

    def test_missing_source_id(self):
        response = self.client.post(
            "/api/projects/p1/metrics/explore",
            json={"metric_key": "charges.amount", "aggregation": "sum"},
            headers={"X-Project-Id": "p1"},
        )
        assert response.status_code == 400
