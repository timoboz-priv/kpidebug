from datetime import datetime, timezone

import stripe

from kpidebug.data.connector import (
    ConnectorError,
    DataSourceConnector,
    MetricDescriptor,
    TableDescriptor,
)
from kpidebug.data.types import (
    DataRecord,
    DataSourceType,
    DimensionValue,
)
from kpidebug.data.stripe.metrics import (
    METRIC_RESOURCE_MAP,
    STRIPE_METRICS,
    STRIPE_METRICS_BY_KEY,
)
from kpidebug.data.stripe.tables import (
    STRIPE_TABLES,
    STRIPE_TABLES_BY_KEY,
)


class StripeConnector(DataSourceConnector):
    source_type: DataSourceType = DataSourceType.STRIPE

    def validate_credentials(
        self, credentials: dict[str, str],
    ) -> bool:
        api_key = credentials.get("api_key", "")
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

    # --- Metrics ---

    def discover_metrics(self) -> list[MetricDescriptor]:
        return list(STRIPE_METRICS)

    def fetch_metric_data(
        self,
        credentials: dict[str, str],
        metric_keys: list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[DataRecord]:
        client = _make_client(credentials)

        if metric_keys:
            requested = metric_keys
        else:
            requested = [m.key for m in STRIPE_METRICS]

        for key in requested:
            if key not in STRIPE_METRICS_BY_KEY:
                raise ConnectorError(
                    f"Unknown metric: {key}"
                )

        resources = set()
        for key in requested:
            group = key.split(".")[0]
            if group in METRIC_RESOURCE_MAP:
                resources.add(group)

        created = _build_created_filter(
            start_time, end_time
        )
        records: list[DataRecord] = []
        field_set = set(requested)

        if "charges" in resources:
            records.extend(
                _metric_charges(client, field_set, created)
            )
        if "customers" in resources:
            records.extend(
                _metric_customers(client, field_set, created)
            )
        if "subscriptions" in resources:
            records.extend(
                _metric_subs(client, field_set, created)
            )
        if "invoices" in resources:
            records.extend(
                _metric_invoices(client, field_set, created)
            )
        if "refunds" in resources:
            records.extend(
                _metric_refunds(client, field_set, created)
            )

        return records

    # --- Tables ---

    def discover_tables(self) -> list[TableDescriptor]:
        return list(STRIPE_TABLES)

    def fetch_table_data(
        self,
        credentials: dict[str, str],
        table_key: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict]:
        if table_key not in STRIPE_TABLES_BY_KEY:
            raise ConnectorError(
                f"Unknown table: {table_key}"
            )
        client = _make_client(credentials)
        created = _build_created_filter(
            start_time, end_time
        )

        fetchers = {
            "charges": _table_charges,
            "customers": _table_customers,
            "subscriptions": _table_subscriptions,
            "invoices": _table_invoices,
            "refunds": _table_refunds,
        }
        fetch_fn = fetchers[table_key]
        return fetch_fn(client, created)


# --- Helpers ---

def _make_client(
    credentials: dict[str, str],
) -> stripe.StripeClient:
    api_key = credentials.get("api_key", "")
    if not api_key:
        raise ConnectorError(
            "Missing api_key in credentials"
        )
    return stripe.StripeClient(api_key)


def _build_created_filter(
    start_time: str | None,
    end_time: str | None,
) -> dict[str, int] | None:
    if not start_time and not end_time:
        return None
    created: dict[str, int] = {}
    if start_time:
        dt = datetime.fromisoformat(start_time)
        created["gte"] = int(dt.timestamp())
    if end_time:
        dt = datetime.fromisoformat(end_time)
        created["lte"] = int(dt.timestamp())
    return created


def _ts_to_iso(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _list_all(list_fn, params: dict) -> list:
    """Auto-paginate a Stripe list endpoint."""
    items = []
    params["limit"] = 100
    result = list_fn(params=params)
    items.extend(result.data)
    while result.has_more:
        params["starting_after"] = result.data[-1].id
        result = list_fn(params=params)
        items.extend(result.data)
    return items


def _stripe_params(
    created: dict[str, int] | None, extra: dict | None = None,
) -> dict:
    params: dict = {}
    if created:
        params["created"] = created
    if extra:
        params.update(extra)
    return params


# --- Metric data fetchers ---

def _metric_charges(
    client: stripe.StripeClient,
    field_set: set[str],
    created: dict[str, int] | None,
) -> list[DataRecord]:
    charges = _list_all(
        client.v1.charges.list, _stripe_params(created)
    )
    records: list[DataRecord] = []
    for charge in charges:
        ts = _ts_to_iso(charge.created)
        dims = [
            DimensionValue(dimension="time", value=ts),
            DimensionValue(
                dimension="currency",
                value=charge.currency or "",
            ),
            DimensionValue(
                dimension="charge_status",
                value=charge.status or "",
            ),
        ]
        if "charges.amount" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="charges.amount",
                value=float(charge.amount or 0),
                timestamp=ts,
                dimension_values=dims,
            ))
        if "charges.count" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="charges.count",
                value=1.0,
                timestamp=ts,
                dimension_values=dims,
            ))
    return records


def _metric_customers(
    client: stripe.StripeClient,
    field_set: set[str],
    created: dict[str, int] | None,
) -> list[DataRecord]:
    if "customers.count" not in field_set:
        return []
    customers = _list_all(
        client.v1.customers.list, _stripe_params(created)
    )
    records: list[DataRecord] = []
    for customer in customers:
        ts = _ts_to_iso(customer.created)
        records.append(DataRecord(
            source_type=DataSourceType.STRIPE,
            field="customers.count",
            value=1.0,
            timestamp=ts,
            dimension_values=[
                DimensionValue(dimension="time", value=ts),
            ],
        ))
    return records


def _metric_subs(
    client: stripe.StripeClient,
    field_set: set[str],
    created: dict[str, int] | None,
) -> list[DataRecord]:
    subs = _list_all(
        client.v1.subscriptions.list,
        _stripe_params(created, {"status": "all"}),
    )
    records: list[DataRecord] = []
    for sub in subs:
        ts = _ts_to_iso(sub.created)
        plan_amount = 0
        currency = ""
        interval = ""
        if sub.items and sub.items.data:
            item = sub.items.data[0]
            if item.price:
                plan_amount = item.price.unit_amount or 0
                currency = item.price.currency or ""
                interval = (
                    item.price.recurring.interval
                    if item.price.recurring else ""
                )
        dims = [
            DimensionValue(dimension="time", value=ts),
            DimensionValue(
                dimension="subscription_status",
                value=sub.status or "",
            ),
            DimensionValue(
                dimension="subscription_interval",
                value=interval,
            ),
        ]
        if "subscriptions.count" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="subscriptions.count",
                value=1.0, timestamp=ts,
                dimension_values=dims,
            ))
        if "subscriptions.amount" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="subscriptions.amount",
                value=float(plan_amount), timestamp=ts,
                dimension_values=dims + [
                    DimensionValue(
                        dimension="currency",
                        value=currency,
                    ),
                ],
            ))
    return records


def _metric_invoices(
    client: stripe.StripeClient,
    field_set: set[str],
    created: dict[str, int] | None,
) -> list[DataRecord]:
    invoices = _list_all(
        client.v1.invoices.list, _stripe_params(created)
    )
    records: list[DataRecord] = []
    for inv in invoices:
        ts = _ts_to_iso(inv.created)
        dims = [
            DimensionValue(dimension="time", value=ts),
            DimensionValue(
                dimension="currency",
                value=inv.currency or "",
            ),
            DimensionValue(
                dimension="invoice_status",
                value=inv.status or "",
            ),
        ]
        if "invoices.amount_due" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="invoices.amount_due",
                value=float(inv.amount_due or 0),
                timestamp=ts, dimension_values=dims,
            ))
        if "invoices.amount_paid" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="invoices.amount_paid",
                value=float(inv.amount_paid or 0),
                timestamp=ts, dimension_values=dims,
            ))
        if "invoices.count" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="invoices.count",
                value=1.0, timestamp=ts,
                dimension_values=dims,
            ))
    return records


def _metric_refunds(
    client: stripe.StripeClient,
    field_set: set[str],
    created: dict[str, int] | None,
) -> list[DataRecord]:
    refunds = _list_all(
        client.v1.refunds.list, _stripe_params(created)
    )
    records: list[DataRecord] = []
    for refund in refunds:
        ts = _ts_to_iso(refund.created)
        dims = [
            DimensionValue(dimension="time", value=ts),
            DimensionValue(
                dimension="currency",
                value=refund.currency or "",
            ),
        ]
        if "refunds.amount" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="refunds.amount",
                value=float(refund.amount or 0),
                timestamp=ts, dimension_values=dims,
            ))
        if "refunds.count" in field_set:
            records.append(DataRecord(
                source_type=DataSourceType.STRIPE,
                field="refunds.count",
                value=1.0, timestamp=ts,
                dimension_values=dims,
            ))
    return records


# --- Table data fetchers ---

def _table_charges(
    client: stripe.StripeClient,
    created: dict[str, int] | None,
) -> list[dict]:
    charges = _list_all(
        client.v1.charges.list, _stripe_params(created)
    )
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


def _table_customers(
    client: stripe.StripeClient,
    created: dict[str, int] | None,
) -> list[dict]:
    customers = _list_all(
        client.v1.customers.list, _stripe_params(created)
    )
    return [
        {
            "id": c.id,
            "name": c.name or "",
            "email": c.email or "",
            "created": _ts_to_iso(c.created),
        }
        for c in customers
    ]


def _table_subscriptions(
    client: stripe.StripeClient,
    created: dict[str, int] | None,
) -> list[dict]:
    subs = _list_all(
        client.v1.subscriptions.list,
        _stripe_params(created, {"status": "all"}),
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


def _table_invoices(
    client: stripe.StripeClient,
    created: dict[str, int] | None,
) -> list[dict]:
    invoices = _list_all(
        client.v1.invoices.list, _stripe_params(created)
    )
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


def _table_refunds(
    client: stripe.StripeClient,
    created: dict[str, int] | None,
) -> list[dict]:
    refunds = _list_all(
        client.v1.refunds.list, _stripe_params(created)
    )
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
