from unittest.mock import MagicMock, patch

import pytest
import stripe

from kpidebug.data.connector import ConnectorError
from kpidebug.data.stripe.connector import StripeConnector


class TestStripeConnectorValidation:
    def setup_method(self):
        self.connector = StripeConnector()

    def test_missing_api_key(self):
        with pytest.raises(ConnectorError, match="Missing api_key"):
            self.connector.validate_credentials({})

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_valid_api_key(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        result = self.connector.validate_credentials(
            {"api_key": "sk_test_valid"}
        )
        assert result is True

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_invalid_api_key(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.v1.balance.retrieve.side_effect = (
            stripe.AuthenticationError("Invalid")
        )
        with pytest.raises(ConnectorError, match="Invalid Stripe API key"):
            self.connector.validate_credentials(
                {"api_key": "sk_test_bad"}
            )


class TestStripeConnectorDiscoverMetrics:
    def test_returns_all_metrics(self):
        connector = StripeConnector()
        metrics = connector.discover_metrics()
        assert len(metrics) > 0
        keys = [m.key for m in metrics]
        assert "charges.amount" in keys
        assert "charges.count" in keys
        assert "customers.count" in keys
        assert "subscriptions.count" in keys

    def test_metrics_have_dimensions(self):
        connector = StripeConnector()
        metrics = connector.discover_metrics()
        for metric in metrics:
            assert len(metric.dimensions) > 0
            dim_names = [d.name for d in metric.dimensions]
            assert "time" in dim_names


class TestStripeConnectorDiscoverTables:
    def test_returns_all_tables(self):
        connector = StripeConnector()
        tables = connector.discover_tables()
        keys = [t.key for t in tables]
        assert "charges" in keys
        assert "customers" in keys
        assert "subscriptions" in keys
        assert "invoices" in keys
        assert "refunds" in keys

    def test_tables_have_columns(self):
        connector = StripeConnector()
        tables = connector.discover_tables()
        for table in tables:
            assert len(table.columns) > 0


class TestStripeConnectorFetchMetricData:
    def setup_method(self):
        self.connector = StripeConnector()

    def test_missing_credentials(self):
        with pytest.raises(ConnectorError, match="Missing api_key"):
            self.connector.fetch_metric_data({})

    def test_unknown_metric(self):
        with pytest.raises(ConnectorError, match="Unknown metric"):
            self.connector.fetch_metric_data(
                {"api_key": "sk_test"},
                metric_keys=["nonexistent.metric"],
            )

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_charges(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        charge = MagicMock()
        charge.id = "ch_1"
        charge.amount = 5000
        charge.currency = "usd"
        charge.status = "succeeded"
        charge.created = 1711929600

        list_result = MagicMock()
        list_result.data = [charge]
        list_result.has_more = False
        mock_client.v1.charges.list.return_value = list_result

        records = self.connector.fetch_metric_data(
            credentials={"api_key": "sk_test"},
            metric_keys=["charges.amount", "charges.count"],
        )

        assert len(records) == 2
        amount_rec = [r for r in records if r.field == "charges.amount"][0]
        count_rec = [r for r in records if r.field == "charges.count"][0]
        assert amount_rec.value == 5000.0
        assert count_rec.value == 1.0

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_customers(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        customer = MagicMock()
        customer.id = "cus_1"
        customer.created = 1711929600

        list_result = MagicMock()
        list_result.data = [customer]
        list_result.has_more = False
        mock_client.v1.customers.list.return_value = list_result

        records = self.connector.fetch_metric_data(
            credentials={"api_key": "sk_test"},
            metric_keys=["customers.count"],
        )
        assert len(records) == 1
        assert records[0].field == "customers.count"
        assert records[0].value == 1.0


class TestStripeConnectorFetchTableData:
    def setup_method(self):
        self.connector = StripeConnector()

    def test_unknown_table(self):
        with pytest.raises(ConnectorError, match="Unknown table"):
            self.connector.fetch_table_data(
                {"api_key": "sk_test"}, "nonexistent"
            )

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_charges_table(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        charge = MagicMock()
        charge.id = "ch_1"
        charge.amount = 5000
        charge.currency = "usd"
        charge.status = "succeeded"
        charge.description = "Test charge"
        charge.customer = "cus_1"
        charge.created = 1711929600

        list_result = MagicMock()
        list_result.data = [charge]
        list_result.has_more = False
        mock_client.v1.charges.list.return_value = list_result

        rows = self.connector.fetch_table_data(
            {"api_key": "sk_test"}, "charges"
        )

        assert len(rows) == 1
        assert rows[0]["id"] == "ch_1"
        assert rows[0]["amount"] == 5000
        assert rows[0]["currency"] == "usd"
        assert rows[0]["status"] == "succeeded"

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_customers_table(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        customer = MagicMock()
        customer.id = "cus_1"
        customer.name = "Alice"
        customer.email = "alice@test.com"
        customer.created = 1711929600

        list_result = MagicMock()
        list_result.data = [customer]
        list_result.has_more = False
        mock_client.v1.customers.list.return_value = list_result

        rows = self.connector.fetch_table_data(
            {"api_key": "sk_test"}, "customers"
        )

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["email"] == "alice@test.com"
