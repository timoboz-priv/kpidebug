from unittest.mock import MagicMock, patch

import pytest
import stripe

from kpidebug.data.connector import ConnectorError
from kpidebug.data.types import DataSource, DataSourceType, TableFilter, TableQuery
from kpidebug.data.stripe.connector import StripeConnector


def _make_connector(api_key: str = "sk_test") -> StripeConnector:
    source = DataSource(type=DataSourceType.STRIPE, credentials={"api_key": api_key})
    return StripeConnector(source)


def _make_connector_no_key() -> StripeConnector:
    source = DataSource(type=DataSourceType.STRIPE, credentials={})
    return StripeConnector(source)


class TestStripeConnectorValidation:
    def test_missing_api_key(self):
        connector = _make_connector_no_key()
        with pytest.raises(ConnectorError, match="Missing api_key"):
            connector.validate_credentials()

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_valid_api_key(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        connector = _make_connector("sk_test_valid")
        result = connector.validate_credentials()
        assert result is True

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_invalid_api_key(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.v1.balance.retrieve.side_effect = (
            stripe.AuthenticationError("Invalid")
        )
        connector = _make_connector("sk_test_bad")
        with pytest.raises(ConnectorError, match="Invalid Stripe API key"):
            connector.validate_credentials()


class TestStripeConnectorGetTables:
    def test_returns_all_tables(self):
        connector = _make_connector()
        tables = connector.get_tables()
        keys = [t.key for t in tables]
        assert "charges" in keys
        assert "customers" in keys
        assert "subscriptions" in keys
        assert "invoices" in keys
        assert "refunds" in keys

    def test_tables_have_columns(self):
        connector = _make_connector()
        tables = connector.get_tables()
        for table in tables:
            assert len(table.columns) > 0


def _mock_charges(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    charges = []
    for i, (amount, currency, status) in enumerate([
        (5000, "usd", "succeeded"),
        (3000, "eur", "succeeded"),
        (1000, "usd", "failed"),
    ]):
        c = MagicMock()
        c.id = f"ch_{i}"
        c.amount = amount
        c.currency = currency
        c.status = status
        c.description = f"Charge {i}"
        c.customer = f"cus_{i}"
        c.created = 1711929600 + i
        charges.append(c)

    list_result = MagicMock()
    list_result.data = charges
    list_result.has_more = False
    mock_client.v1.charges.list.return_value = list_result
    return mock_client


class TestStripeConnectorFetchTableData:
    def test_unknown_table(self):
        connector = _make_connector()
        with pytest.raises(ConnectorError, match="Unknown table"):
            connector.fetch_table_data("nonexistent")

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_returns_table_result(self, mock_client_cls):
        _mock_charges(mock_client_cls)
        connector = _make_connector()
        result = connector.fetch_table_data("charges")

        assert result.total_count == 3
        assert len(result.rows) == 3
        assert result.rows[0]["id"] == "ch_0"

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_with_filter(self, mock_client_cls):
        _mock_charges(mock_client_cls)
        connector = _make_connector()
        query = TableQuery(
            filters=[TableFilter(column="currency", operator="eq", value="usd")],
        )
        result = connector.fetch_table_data("charges", query)

        assert result.total_count == 2
        assert all(r["currency"] == "usd" for r in result.rows)

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_with_sort(self, mock_client_cls):
        _mock_charges(mock_client_cls)
        connector = _make_connector()
        query = TableQuery(sort_by="amount", sort_order="desc")
        result = connector.fetch_table_data("charges", query)

        amounts = [r["amount"] for r in result.rows]
        assert amounts == [5000, 3000, 1000]

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_with_pagination(self, mock_client_cls):
        _mock_charges(mock_client_cls)
        connector = _make_connector()
        query = TableQuery(limit=2, offset=0)
        result = connector.fetch_table_data("charges", query)

        assert result.total_count == 3
        assert len(result.rows) == 2

    @patch("kpidebug.data.stripe.connector.stripe.StripeClient")
    def test_fetch_all_rows(self, mock_client_cls):
        _mock_charges(mock_client_cls)
        connector = _make_connector()
        rows = connector.fetch_all_rows("charges")

        assert len(rows) == 3

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

        connector = _make_connector()
        result = connector.fetch_table_data("customers")

        assert result.total_count == 1
        assert result.rows[0]["name"] == "Alice"
        assert result.rows[0]["email"] == "alice@test.com"
