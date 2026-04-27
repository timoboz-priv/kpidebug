from kpidebug.data.types import Aggregation, DataSourceType, TableFilter
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.registry import register
from kpidebug.metrics.types import Metric, MetricDataType, MetricResult, MetricDimension, apply_time_filter, parse_group_key


def _apply_filters(table, filters):
    if not filters:
        return table
    for f in filters:
        table = table.filter(f.column, f.operator, f.value)
    return table


class GrossRevenueMetric(Metric):
    id = "builtin:stripe.gross_revenue"
    name = "Gross Revenue"
    description = "Total charge amount for successful payments"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:charges"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
        MetricDimension(key="card_brand", name="Card Brand"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="disputed", name="Disputed"),
        MetricDimension(key="captured", name="Captured"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:charges"), "created", days, date)
        table = table.filter("paid", "eq", "true")
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("amount", aggregation).items())]
        return [MetricResult(value=table.aggregate("amount", aggregation))]


class NetRevenueMetric(Metric):
    id = "builtin:stripe.net_revenue"
    name = "Net Revenue"
    description = "Revenue after Stripe fees (from balance transactions)"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:balance_transactions"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="reporting_category", name="Reporting Category"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="type", name="Transaction Type"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:balance_transactions"), "created", days, date)
        table = table.filter("type", "eq", "charge")
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("net", aggregation).items())]
        return [MetricResult(value=table.aggregate("net", aggregation))]


class CustomerCountMetric(Metric):
    id = "builtin:stripe.customer_count"
    name = "Customer Count"
    description = "Total number of customers"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:customers"]
    dimensions = [
        MetricDimension(key="country", name="Country"),
        MetricDimension(key="city", name="City"),
        MetricDimension(key="delinquent", name="Delinquent"),
        MetricDimension(key="tax_exempt", name="Tax Exempt"),
        MetricDimension(key="currency", name="Currency"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:customers"), "created", days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("id", Aggregation.COUNT).items())]
        return [MetricResult(value=float(table.count()))]


class MrrMetric(Metric):
    id = "builtin:stripe.mrr"
    name = "Monthly Recurring Revenue"
    description = "Sum of active subscription amounts (monthly basis)"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:subscriptions"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="interval", name="Billing Interval"),
        MetricDimension(key="cancel_at_period_end", name="Canceling"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:subscriptions"), "created", days, date)
        table = table.filter("status", "eq", "active")
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("amount", aggregation).items())]
        return [MetricResult(value=table.aggregate("amount", aggregation))]


class RefundRateMetric(Metric):
    id = "builtin:stripe.refund_rate"
    name = "Refund Rate"
    description = "Percentage of charge amount that was refunded"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.STRIPE
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["stripe:charges"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:charges"), "created", days, date)
        table = table.filter("paid", "eq", "true")
        table = _apply_filters(table, filters)
        total = table.aggregate("amount", Aggregation.SUM)
        refunded = table.aggregate("amount_refunded", Aggregation.SUM)
        value = (refunded / total * 100) if total > 0 else 0.0
        return [MetricResult(value=value)]


class TotalFeesMetric(Metric):
    id = "builtin:stripe.total_fees"
    name = "Total Fees"
    description = "Total Stripe processing fees"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:balance_transactions"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="type", name="Transaction Type"),
        MetricDimension(key="reporting_category", name="Reporting Category"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:balance_transactions"), "created", days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("fee", aggregation).items())]
        return [MetricResult(value=table.aggregate("fee", aggregation))]


class RefundVolumeMetric(Metric):
    id = "builtin:stripe.refund_volume"
    name = "Refund Volume"
    description = "Total amount refunded"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:refunds"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="reason", name="Reason"),
        MetricDimension(key="status", name="Status"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:refunds"), "created", days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("amount", aggregation).items())]
        return [MetricResult(value=table.aggregate("amount", aggregation))]


class DisputeCountMetric(Metric):
    id = "builtin:stripe.dispute_count"
    name = "Dispute Count"
    description = "Number of payment disputes and chargebacks"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:disputes"]
    dimensions = [
        MetricDimension(key="reason", name="Reason"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="currency", name="Currency"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:disputes"), "created", days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("id", Aggregation.COUNT).items())]
        return [MetricResult(value=float(table.count()))]


class InvoiceCollectionRateMetric(Metric):
    id = "builtin:stripe.invoice_collection_rate"
    name = "Invoice Collection Rate"
    description = "Percentage of invoiced amounts that were paid"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.STRIPE
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["stripe:invoices"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="collection_method", name="Collection Method"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:invoices"), "created", days, date)
        table = _apply_filters(table, filters)
        paid = table.aggregate("amount_paid", Aggregation.SUM)
        due = table.aggregate("amount_due", Aggregation.SUM)
        value = (paid / due * 100) if due > 0 else 0.0
        return [MetricResult(value=value)]


class PayoutVolumeMetric(Metric):
    id = "builtin:stripe.payout_volume"
    name = "Payout Volume"
    description = "Total amount paid out to your bank"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:payouts"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="type", name="Payout Type"),
        MetricDimension(key="method", name="Payout Method"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:payouts"), "created", days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("amount", aggregation).items())]
        return [MetricResult(value=table.aggregate("amount", aggregation))]


register(GrossRevenueMetric())
register(NetRevenueMetric())
register(CustomerCountMetric())
register(MrrMetric())
register(RefundRateMetric())
register(TotalFeesMetric())
register(RefundVolumeMetric())
register(DisputeCountMetric())
register(InvoiceCollectionRateMetric())
register(PayoutVolumeMetric())
