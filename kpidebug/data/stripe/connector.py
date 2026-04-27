from datetime import datetime, timezone
from typing import Callable

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


_FETCHERS: dict[str, Callable] = {}


def _register(key: str):
    def decorator(fn: Callable):
        _FETCHERS[key] = fn
        return fn
    return decorator


class StripeConnector(DataSourceConnector):

    def __init__(self, source: DataSource):
        super().__init__(source)

    def validate_credentials(self) -> bool:
        api_key = self.source.credentials.get("api_key", "")
        if not api_key:
            raise ConnectorError("Missing api_key in credentials")
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
        rows = self.fetch_all_rows(table_key)

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
            raise ConnectorError(f"Unknown table: {table_key}")
        fetcher = _FETCHERS.get(table_key)
        if fetcher is None:
            raise ConnectorError(f"No fetcher for table: {table_key}")
        return fetcher(self._make_client())

    def _make_client(self) -> stripe.StripeClient:
        api_key = self.source.credentials.get("api_key", "")
        if not api_key:
            raise ConnectorError("Missing api_key in credentials")
        return stripe.StripeClient(api_key)


# --- Helpers ---

def _attr(obj: object, name: str, default: object = "") -> object:
    try:
        val = getattr(obj, name, None)
        return val if val is not None else default
    except AttributeError:
        return default


def _ts(obj: object, name: str) -> str:
    val = _attr(obj, name, None)
    if val and isinstance(val, int):
        return _ts_to_iso(val)
    return ""


def _ts_to_iso(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _list_all(list_fn: Callable, params: dict) -> list:
    items: list = []
    params["limit"] = 100
    result = list_fn(params=params)
    items.extend(result.data)
    while result.has_more:
        params["starting_after"] = result.data[-1].id
        result = list_fn(params=params)
        items.extend(result.data)
    return items


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
    elif f.operator in ("gt", "gte", "lt", "lte"):
        ops = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
        try:
            fval, ftarget = float(val), float(target)
        except ValueError:
            fval, ftarget = None, None
        if fval is not None and ftarget is not None:
            return eval(f"{fval} {ops[f.operator]} {ftarget}")
        return eval(f"val {ops[f.operator]} target")
    return True


def _apply_sort(rows: list[dict], sort_by: str | None, sort_order: str) -> list[dict]:
    if not sort_by:
        return rows
    return sorted(rows, key=lambda r: r.get(sort_by, ""), reverse=(sort_order == "desc"))


# --- Table fetchers ---

@_register("stripe:charges")
def _table_charges(client: stripe.StripeClient) -> list[dict]:
    charges = _list_all(client.v1.charges.list, {})
    rows: list[dict] = []
    for c in charges:
        pmd = _attr(c, "payment_method_details", None)
        card = getattr(pmd, "card", None) if pmd else None
        rows.append({
            "id": c.id,
            "amount": c.amount or 0,
            "amount_captured": _attr(c, "amount_captured", 0),
            "amount_refunded": _attr(c, "amount_refunded", 0),
            "currency": c.currency or "",
            "status": c.status or "",
            "paid": bool(_attr(c, "paid", False)),
            "captured": bool(_attr(c, "captured", False)),
            "refunded": bool(_attr(c, "refunded", False)),
            "disputed": bool(_attr(c, "disputed", False)),
            "description": c.description or "",
            "customer": c.customer or "",
            "invoice": _attr(c, "invoice", ""),
            "payment_intent": _attr(c, "payment_intent", ""),
            "payment_method_type": str(_attr(pmd, "type", "")) if pmd else "",
            "card_brand": str(_attr(card, "brand", "")) if card else "",
            "card_last4": str(_attr(card, "last4", "")) if card else "",
            "failure_code": _attr(c, "failure_code", ""),
            "failure_message": _attr(c, "failure_message", ""),
            "receipt_email": _attr(c, "receipt_email", ""),
            "created": _ts_to_iso(c.created),
        })
    return rows


@_register("stripe:customers")
def _table_customers(client: stripe.StripeClient) -> list[dict]:
    customers = _list_all(client.v1.customers.list, {})
    rows: list[dict] = []
    for c in customers:
        address = _attr(c, "address", None)
        rows.append({
            "id": c.id,
            "name": c.name or "",
            "email": c.email or "",
            "phone": _attr(c, "phone", ""),
            "description": _attr(c, "description", ""),
            "balance": _attr(c, "balance", 0),
            "currency": _attr(c, "currency", ""),
            "delinquent": bool(_attr(c, "delinquent", False)),
            "country": str(_attr(address, "country", "")) if address else "",
            "city": str(_attr(address, "city", "")) if address else "",
            "tax_exempt": _attr(c, "tax_exempt", ""),
            "created": _ts_to_iso(c.created),
        })
    return rows


@_register("stripe:subscriptions")
def _table_subscriptions(client: stripe.StripeClient) -> list[dict]:
    subs = _list_all(client.v1.subscriptions.list, {"status": "all"})
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
            "customer": _attr(s, "customer", ""),
            "status": s.status or "",
            "amount": amount,
            "currency": currency,
            "interval": interval,
            "cancel_at_period_end": bool(_attr(s, "cancel_at_period_end", False)),
            "canceled_at": _ts(s, "canceled_at"),
            "cancel_at": _ts(s, "cancel_at"),
            "trial_start": _ts(s, "trial_start"),
            "trial_end": _ts(s, "trial_end"),
            "current_period_start": _ts(s, "current_period_start"),
            "current_period_end": _ts(s, "current_period_end"),
            "description": _attr(s, "description", ""),
            "default_payment_method": _attr(s, "default_payment_method", ""),
            "latest_invoice": _attr(s, "latest_invoice", ""),
            "start_date": _ts(s, "start_date"),
            "created": _ts_to_iso(s.created),
        })
    return rows


@_register("stripe:invoices")
def _table_invoices(client: stripe.StripeClient) -> list[dict]:
    invoices = _list_all(client.v1.invoices.list, {})
    return [
        {
            "id": i.id,
            "number": _attr(i, "number", ""),
            "amount_due": i.amount_due or 0,
            "amount_paid": i.amount_paid or 0,
            "amount_remaining": _attr(i, "amount_remaining", 0),
            "subtotal": _attr(i, "subtotal", 0),
            "total": _attr(i, "total", 0),
            "currency": i.currency or "",
            "status": i.status or "",
            "paid": bool(_attr(i, "paid", False)),
            "customer": i.customer or "",
            "subscription": _attr(i, "subscription", ""),
            "collection_method": _attr(i, "collection_method", ""),
            "attempt_count": _attr(i, "attempt_count", 0),
            "due_date": _ts(i, "due_date"),
            "period_start": _ts(i, "period_start"),
            "period_end": _ts(i, "period_end"),
            "created": _ts_to_iso(i.created),
        }
        for i in invoices
    ]


@_register("stripe:refunds")
def _table_refunds(client: stripe.StripeClient) -> list[dict]:
    refunds = _list_all(client.v1.refunds.list, {})
    return [
        {
            "id": r.id,
            "amount": r.amount or 0,
            "currency": r.currency or "",
            "status": r.status or "",
            "reason": _attr(r, "reason", ""),
            "charge": r.charge or "",
            "payment_intent": _attr(r, "payment_intent", ""),
            "failure_reason": _attr(r, "failure_reason", ""),
            "description": _attr(r, "description", ""),
            "created": _ts_to_iso(r.created),
        }
        for r in refunds
    ]


@_register("stripe:balance_transactions")
def _table_balance_transactions(client: stripe.StripeClient) -> list[dict]:
    txns = _list_all(client.v1.balance_transactions.list, {})
    return [
        {
            "id": t.id,
            "amount": t.amount or 0,
            "fee": _attr(t, "fee", 0),
            "net": _attr(t, "net", 0),
            "currency": t.currency or "",
            "type": _attr(t, "type", ""),
            "status": _attr(t, "status", ""),
            "source": _attr(t, "source", ""),
            "description": _attr(t, "description", ""),
            "reporting_category": _attr(t, "reporting_category", ""),
            "available_on": _ts(t, "available_on"),
            "created": _ts_to_iso(t.created),
        }
        for t in txns
    ]


@_register("stripe:disputes")
def _table_disputes(client: stripe.StripeClient) -> list[dict]:
    disputes = _list_all(client.v1.disputes.list, {})
    return [
        {
            "id": d.id,
            "amount": _attr(d, "amount", 0),
            "currency": _attr(d, "currency", ""),
            "charge": _attr(d, "charge", ""),
            "payment_intent": _attr(d, "payment_intent", ""),
            "reason": _attr(d, "reason", ""),
            "status": _attr(d, "status", ""),
            "is_charge_refundable": bool(_attr(d, "is_charge_refundable", False)),
            "created": _ts_to_iso(d.created),
        }
        for d in disputes
    ]


@_register("stripe:products")
def _table_products(client: stripe.StripeClient) -> list[dict]:
    products = _list_all(client.v1.products.list, {})
    return [
        {
            "id": p.id,
            "name": _attr(p, "name", ""),
            "description": _attr(p, "description", ""),
            "active": bool(_attr(p, "active", False)),
            "default_price": _attr(p, "default_price", ""),
            "type": _attr(p, "type", ""),
            "created": _ts_to_iso(p.created),
            "updated": _ts(p, "updated"),
        }
        for p in products
    ]


@_register("stripe:prices")
def _table_prices(client: stripe.StripeClient) -> list[dict]:
    prices = _list_all(client.v1.prices.list, {})
    rows: list[dict] = []
    for p in prices:
        recurring = _attr(p, "recurring", None)
        rows.append({
            "id": p.id,
            "product": _attr(p, "product", ""),
            "active": bool(_attr(p, "active", False)),
            "currency": _attr(p, "currency", ""),
            "unit_amount": _attr(p, "unit_amount", 0),
            "type": _attr(p, "type", ""),
            "recurring_interval": str(_attr(recurring, "interval", "")) if recurring else "",
            "recurring_interval_count": _attr(recurring, "interval_count", 0) if recurring else 0,
            "nickname": _attr(p, "nickname", ""),
            "tax_behavior": _attr(p, "tax_behavior", ""),
            "created": _ts_to_iso(p.created),
        })
    return rows


@_register("stripe:payouts")
def _table_payouts(client: stripe.StripeClient) -> list[dict]:
    payouts = _list_all(client.v1.payouts.list, {})
    return [
        {
            "id": p.id,
            "amount": p.amount or 0,
            "currency": _attr(p, "currency", ""),
            "status": _attr(p, "status", ""),
            "type": _attr(p, "type", ""),
            "method": _attr(p, "method", ""),
            "description": _attr(p, "description", ""),
            "arrival_date": _ts(p, "arrival_date"),
            "created": _ts_to_iso(p.created),
        }
        for p in payouts
    ]
