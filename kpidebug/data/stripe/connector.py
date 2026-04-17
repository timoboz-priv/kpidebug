from datetime import datetime, timezone

import stripe

from kpidebug.data.connector import ConnectorError, DataSourceConnector
from kpidebug.data.types import (
    DataSource,
    TableDescriptor,
    TableFilter,
    TableQuery,
    TableResult,
)
from kpidebug.data.stripe.tables import STRIPE_TABLES, STRIPE_TABLES_BY_KEY


class StripeConnector(DataSourceConnector):

    def __init__(self, source: DataSource):
        super().__init__(source)

    def validate_credentials(self) -> bool:
        api_key = self.source.credentials.get("api_key", "")
        if not api_key:
            raise ConnectorError(
                "Missing api_key in credentials"
            )
        try:
            client = stripe.StripeClient(api_key)
            client.v1.balance.retrieve()
            return True
        except stripe.AuthenticationError:
            raise ConnectorError("Invalid Stripe API key")
        except stripe.StripeError as e:
            raise ConnectorError(f"Stripe error: {e}")

    def get_tables(self) -> list[TableDescriptor]:
        return list(STRIPE_TABLES)

    def fetch_table_data(
        self,
        table_key: str,
        query: TableQuery | None = None,
    ) -> TableResult:
        if table_key not in STRIPE_TABLES_BY_KEY:
            raise ConnectorError(
                f"Unknown table: {table_key}"
            )
        client = self._make_client()

        fetchers = {
            "charges": _table_charges,
            "customers": _table_customers,
            "subscriptions": _table_subscriptions,
            "invoices": _table_invoices,
            "refunds": _table_refunds,
        }
        rows = fetchers[table_key](client)

        if query:
            rows = _apply_filters(rows, query.filters)
            total_count = len(rows)
            rows = _apply_sort(rows, query.sort_by, query.sort_order)
            rows = rows[query.offset:query.offset + query.limit]
        else:
            total_count = len(rows)

        return TableResult(rows=rows, total_count=total_count)

    def fetch_all_rows(self, table_key: str) -> list[dict]:
        if table_key not in STRIPE_TABLES_BY_KEY:
            raise ConnectorError(
                f"Unknown table: {table_key}"
            )
        client = self._make_client()
        fetchers = {
            "charges": _table_charges,
            "customers": _table_customers,
            "subscriptions": _table_subscriptions,
            "invoices": _table_invoices,
            "refunds": _table_refunds,
        }
        return fetchers[table_key](client)

    def _make_client(self) -> stripe.StripeClient:
        api_key = self.source.credentials.get("api_key", "")
        if not api_key:
            raise ConnectorError(
                "Missing api_key in credentials"
            )
        return stripe.StripeClient(api_key)


def _apply_filters(rows: list[dict], filters: list[TableFilter]) -> list[dict]:
    if not filters:
        return rows
    return [row for row in rows if all(_matches(row, f) for f in filters)]


def _matches(row: dict, f: TableFilter) -> bool:
    val = str(row.get(f.column, ""))
    target = f.value

    if f.operator == "eq":
        return val == target
    elif f.operator == "neq":
        return val != target
    elif f.operator == "contains":
        return target.lower() in val.lower()
    elif f.operator == "gt":
        try:
            return float(val) > float(target)
        except ValueError:
            return val > target
    elif f.operator == "gte":
        try:
            return float(val) >= float(target)
        except ValueError:
            return val >= target
    elif f.operator == "lt":
        try:
            return float(val) < float(target)
        except ValueError:
            return val < target
    elif f.operator == "lte":
        try:
            return float(val) <= float(target)
        except ValueError:
            return val <= target

    return True


def _apply_sort(rows: list[dict], sort_by: str | None, sort_order: str) -> list[dict]:
    if not sort_by:
        return rows
    return sorted(
        rows,
        key=lambda r: r.get(sort_by, ""),
        reverse=(sort_order == "desc"),
    )


def _ts_to_iso(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _list_all(list_fn, params: dict) -> list:
    items = []
    params["limit"] = 100
    result = list_fn(params=params)
    items.extend(result.data)
    while result.has_more:
        params["starting_after"] = result.data[-1].id
        result = list_fn(params=params)
        items.extend(result.data)
    return items


def _table_charges(client: stripe.StripeClient) -> list[dict]:
    charges = _list_all(client.v1.charges.list, {})
    return [
        {
            "id": c.id,
            "amount": c.amount or 0,
            "currency": c.currency or "",
            "status": c.status or "",
            "description": c.description or "",
            "customer": c.customer or "",
            "created": _ts_to_iso(c.created),
        }
        for c in charges
    ]


def _table_customers(client: stripe.StripeClient) -> list[dict]:
    customers = _list_all(client.v1.customers.list, {})
    return [
        {
            "id": c.id,
            "name": c.name or "",
            "email": c.email or "",
            "created": _ts_to_iso(c.created),
        }
        for c in customers
    ]


def _table_subscriptions(client: stripe.StripeClient) -> list[dict]:
    subs = _list_all(
        client.v1.subscriptions.list, {"status": "all"},
    )
    rows: list[dict] = []
    for s in subs:
        amount = 0
        currency = ""
        interval = ""
        if s.items and s.items.data:
            item = s.items.data[0]
            if item.price:
                amount = item.price.unit_amount or 0
                currency = item.price.currency or ""
                interval = (
                    item.price.recurring.interval
                    if item.price.recurring else ""
                )
        rows.append({
            "id": s.id,
            "status": s.status or "",
            "amount": amount,
            "currency": currency,
            "interval": interval,
            "created": _ts_to_iso(s.created),
            "current_period_start": (
                _ts_to_iso(s.current_period_start)
                if s.current_period_start else ""
            ),
            "current_period_end": (
                _ts_to_iso(s.current_period_end)
                if s.current_period_end else ""
            ),
        })
    return rows


def _table_invoices(client: stripe.StripeClient) -> list[dict]:
    invoices = _list_all(client.v1.invoices.list, {})
    return [
        {
            "id": i.id,
            "amount_due": i.amount_due or 0,
            "amount_paid": i.amount_paid or 0,
            "currency": i.currency or "",
            "status": i.status or "",
            "customer": i.customer or "",
            "created": _ts_to_iso(i.created),
        }
        for i in invoices
    ]


def _table_refunds(client: stripe.StripeClient) -> list[dict]:
    refunds = _list_all(client.v1.refunds.list, {})
    return [
        {
            "id": r.id,
            "amount": r.amount or 0,
            "currency": r.currency or "",
            "status": r.status or "",
            "charge": r.charge or "",
            "created": _ts_to_iso(r.created),
        }
        for r in refunds
    ]
