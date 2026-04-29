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
    table_keys = ["stripe:charges", "stripe:customers"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
        MetricDimension(key="card_brand", name="Card Brand"),
        MetricDimension(key="status", name="Status"),
        MetricDimension(key="disputed", name="Disputed"),
        MetricDimension(key="captured", name="Captured"),
        MetricDimension(key="country", name="Country"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:charges"), "created", days, date)
        table = table.filter("paid", "eq", "true")
        table = _apply_filters(table, filters)

        needs_country = dimensions and "country" in dimensions
        if not needs_country:
            if dimensions:
                grouped = table.group_by(*dimensions)
                return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                        for k, v in sorted(grouped.aggregate("amount", aggregation).items())]
            return [MetricResult(value=table.aggregate("amount", aggregation))]

        charge_rows = table.to_rows()
        customer_rows = ctx.table("stripe:customers").to_rows()
        cust_country: dict[str, str] = {}
        for row in customer_rows:
            cid = str(row.get("id", ""))
            country = str(row.get("country", "") or "Unknown")
            if cid:
                cust_country[cid] = country

        buckets: dict[str, float] = {}
        other_dims = [d for d in dimensions if d != "country"]
        for row in charge_rows:
            country = cust_country.get(str(row.get("customer", "")), "Unknown")
            parts = [country]
            for d in other_dims:
                parts.append(str(row.get(d, "") or ""))
            key = "|".join(parts)
            amount = float(row.get("amount", 0) or 0)
            buckets[key] = buckets.get(key, 0.0) + amount

        all_dims = ["country"] + other_dims
        return [MetricResult(value=v, groups=parse_group_key(k, all_dims))
                for k, v in sorted(buckets.items())]


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



class FailedPaymentCountMetric(Metric):
    id = "builtin:stripe.failed_payment_count"
    name = "Failed Payments"
    description = "Number of invoices that failed to collect payment"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:invoices"]
    dimensions = [
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="collection_method", name="Collection Method"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:invoices"), "created", days, date)
        table = table.filter("paid", "eq", "false")
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("id", Aggregation.COUNT).items())]
        return [MetricResult(value=float(table.count()))]


class ChurnCountMetric(Metric):
    id = "builtin:stripe.churn_count"
    name = "Churned Subscriptions"
    description = "Number of subscriptions canceled in the period"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:subscriptions"]
    dimensions = [
        MetricDimension(key="interval", name="Billing Interval"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("stripe:subscriptions"), "canceled_at", days, date)
        table = table.filter("status", "eq", "canceled")
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate("id", Aggregation.COUNT).items())]
        return [MetricResult(value=float(table.count()))]


class RetentionRateMetric(Metric):
    id = "builtin:stripe.retention_rate"
    name = "Retention Rate"
    description = "Percentage of subscriptions that are still active"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.STRIPE
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["stripe:subscriptions"]
    dimensions = []

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        from datetime import datetime as dt, timedelta, timezone
        as_of = date or dt.now(timezone.utc).date()
        as_of_str = (as_of + timedelta(days=1)).isoformat() + "T00:00:00Z"

        all_subs = ctx.table("stripe:subscriptions")
        all_subs = all_subs.filter("created", "lt", as_of_str)
        all_subs = _apply_filters(all_subs, filters)
        rows = all_subs.to_rows()

        total = 0
        active = 0
        for row in rows:
            total += 1
            status = str(row.get("status", ""))
            if status == "active":
                active += 1
            elif status == "canceled":
                canceled_at = str(row.get("canceled_at", "") or "")
                if canceled_at and canceled_at > as_of_str:
                    active += 1

        value = (active / total * 100) if total > 0 else 0.0
        return [MetricResult(value=value)]


class RevenueByUserTypeMetric(Metric):
    id = "builtin:stripe.revenue_by_user_type"
    name = "Revenue by User Type"
    description = "Charge revenue split by new vs returning customers based on customer creation date"
    data_type = MetricDataType.CURRENCY
    source_type = DataSourceType.STRIPE
    table_keys = ["stripe:charges", "stripe:customers"]
    dimensions = [
        MetricDimension(key="user_type", name="User Type"),
    ]

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        from datetime import datetime as dt, timedelta, timezone

        as_of = date or dt.now(timezone.utc).date()
        window_start = as_of - timedelta(days=days - 1)
        window_start_str = window_start.isoformat() + "T00:00:00Z"

        table = apply_time_filter(ctx.table("stripe:charges"), "created", days, date)
        table = table.filter("paid", "eq", "true")
        table = _apply_filters(table, filters)
        charge_rows = table.to_rows()

        customer_rows = ctx.table("stripe:customers").to_rows()
        cust_created: dict[str, str] = {}
        for row in customer_rows:
            cid = str(row.get("id", ""))
            created = str(row.get("created", "") or "")
            if cid and created:
                cust_created[cid] = created

        buckets: dict[str, float] = {"new": 0.0, "returning": 0.0}
        for row in charge_rows:
            amount = float(row.get("amount", 0) or 0)
            customer_id = str(row.get("customer", "") or "")
            created = cust_created.get(customer_id, "")
            if created and created >= window_start_str:
                buckets["new"] += amount
            else:
                buckets["returning"] += amount

        if dimensions and "user_type" in dimensions:
            return [
                MetricResult(value=v, groups={"user_type": k})
                for k, v in sorted(buckets.items())
                if v > 0
            ]
        return [MetricResult(value=buckets["new"] + buckets["returning"])]


class Retention30dMetric(Metric):
    id = "builtin:stripe.retention_30d"
    name = "30-Day Retention"
    description = "Percentage of subscriptions created ~30 days ago that are still active"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.STRIPE
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["stripe:subscriptions"]
    dimensions = []

    COHORT_CENTER_DAYS = 30
    COHORT_HALF_WINDOW = 3

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        from datetime import datetime as dt, timedelta, timezone

        as_of = date or dt.now(timezone.utc).date()
        as_of_str = (as_of + timedelta(days=1)).isoformat() + "T00:00:00Z"

        cohort_start = as_of - timedelta(days=self.COHORT_CENTER_DAYS + self.COHORT_HALF_WINDOW)
        cohort_end = as_of - timedelta(days=self.COHORT_CENTER_DAYS - self.COHORT_HALF_WINDOW)
        cohort_start_str = cohort_start.isoformat() + "T00:00:00Z"
        cohort_end_str = (cohort_end + timedelta(days=1)).isoformat() + "T00:00:00Z"

        all_subs = ctx.table("stripe:subscriptions")
        all_subs = all_subs.filter("created", "gte", cohort_start_str)
        all_subs = all_subs.filter("created", "lt", cohort_end_str)
        all_subs = _apply_filters(all_subs, filters)
        rows = all_subs.to_rows()

        total = 0
        active = 0
        for row in rows:
            total += 1
            status = str(row.get("status", ""))
            if status == "active":
                active += 1
            elif status == "canceled":
                canceled_at = str(row.get("canceled_at", "") or "")
                if canceled_at and canceled_at > as_of_str:
                    active += 1

        value = (active / total * 100) if total > 0 else 0.0
        return [MetricResult(value=value)]


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
register(FailedPaymentCountMetric())
register(ChurnCountMetric())
register(RetentionRateMetric())
register(RevenueByUserTypeMetric())
register(Retention30dMetric())
