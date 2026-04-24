from kpidebug.metrics.builtin_metrics import (
    BuiltinMetric,
    MetricDimension,
    builtin_registry,
)


def _sum_field(rows: list[dict], field: str) -> float:
    return sum(float(r.get(field, 0) or 0) for r in rows)


builtin_registry.register(BuiltinMetric(
    key="stripe.gross_revenue",
    name="Gross Revenue",
    description="Total charge amount for successful payments",
    table="charges",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
        MetricDimension(key="card_brand", name="Card Brand"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="disputed", name="Disputed"),
        MetricDimension(key="captured", name="Captured"),
    ],
    row_filter=lambda r: r.get("paid") is True or r.get("paid") == "true",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.net_revenue",
    name="Net Revenue",
    description="Revenue after Stripe fees (from balance transactions)",
    table="balance_transactions",
    data_type="currency",
    value_field="net",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="reporting_category", name="Reporting Category"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="type", name="Transaction Type"),
    ],
    row_filter=lambda r: r.get("type") == "charge",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.customer_count",
    name="Customer Count",
    description="Total number of customers",
    table="customers",
    data_type="number",
    value_field="id",
    dimensions=[
        MetricDimension(key="country", name="Country"),
        MetricDimension(key="city", name="City"),
        MetricDimension(key="delinquent", name="Delinquent"),
        MetricDimension(key="tax_exempt", name="Tax Exempt"),
        MetricDimension(key="currency", name="Currency"),
    ],
    compute_fn=lambda rows: float(len(rows)),
))

builtin_registry.register(BuiltinMetric(
    key="stripe.mrr",
    name="Monthly Recurring Revenue",
    description="Sum of active subscription amounts (monthly basis)",
    table="subscriptions",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="interval", name="Billing Interval"),
        MetricDimension(key="cancel_at_period_end", name="Canceling"),
    ],
    row_filter=lambda r: r.get("status") == "active",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.refund_rate",
    name="Refund Rate",
    description="Percentage of charge amount that was refunded",
    table="charges",
    data_type="percent",
    value_field="amount_refunded",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
    ],
    compute_fn=lambda rows: (
        (_sum_field(rows, "amount_refunded") / _sum_field(rows, "amount") * 100)
        if _sum_field(rows, "amount") > 0 else 0.0
    ),
    row_filter=lambda r: r.get("paid") is True or r.get("paid") == "true",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.total_fees",
    name="Total Fees",
    description="Total Stripe processing fees",
    table="balance_transactions",
    data_type="currency",
    value_field="fee",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="type", name="Transaction Type"),
        MetricDimension(key="reporting_category", name="Reporting Category"),
    ],
))

builtin_registry.register(BuiltinMetric(
    key="stripe.refund_volume",
    name="Refund Volume",
    description="Total amount refunded",
    table="refunds",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="reason", name="Reason"),
        MetricDimension(key="status", name="Status"),
    ],
))

builtin_registry.register(BuiltinMetric(
    key="stripe.dispute_count",
    name="Dispute Count",
    description="Number of payment disputes and chargebacks",
    table="disputes",
    data_type="number",
    value_field="id",
    dimensions=[
        MetricDimension(key="reason", name="Reason"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="currency", name="Currency"),
    ],
    compute_fn=lambda rows: float(len(rows)),
))

builtin_registry.register(BuiltinMetric(
    key="stripe.invoice_collection_rate",
    name="Invoice Collection Rate",
    description="Percentage of invoiced amounts that were paid",
    table="invoices",
    data_type="percent",
    value_field="amount_paid",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="collection_method", name="Collection Method"),
    ],
    compute_fn=lambda rows: (
        (_sum_field(rows, "amount_paid") / _sum_field(rows, "amount_due") * 100)
        if _sum_field(rows, "amount_due") > 0 else 0.0
    ),
))

builtin_registry.register(BuiltinMetric(
    key="stripe.payout_volume",
    name="Payout Volume",
    description="Total amount paid out to your bank",
    table="payouts",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="type", name="Payout Type"),
        MetricDimension(key="method", name="Payout Method"),
    ],
))
