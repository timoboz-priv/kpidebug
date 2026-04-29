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


TRAFFIC_DIMS = [
    MetricDimension(key="session_source", name="Source"),
    MetricDimension(key="session_medium", name="Medium"),
    MetricDimension(key="session_channel_group", name="Channel"),
    MetricDimension(key="session_campaign", name="Campaign"),
]

GEO_DIMS = [
    MetricDimension(key="country", name="Country"),
    MetricDimension(key="continent", name="Continent"),
    MetricDimension(key="region", name="Region"),
    MetricDimension(key="city", name="City"),
    MetricDimension(key="language", name="Language"),
]

DEVICE_DIMS = [
    MetricDimension(key="device_category", name="Device"),
    MetricDimension(key="browser", name="Browser"),
    MetricDimension(key="operating_system", name="OS"),
]

USER_DIMS = [MetricDimension(key="new_vs_returning", name="New vs Returning")]
PAGE_DIMS = [MetricDimension(key="page_path", name="Page Path"), MetricDimension(key="page_title", name="Page Title")]
EVENT_DIMS = [MetricDimension(key="event_name", name="Event Name")]
ACQUISITION_DIMS = [
    MetricDimension(key="first_user_source", name="Source"),
    MetricDimension(key="first_user_medium", name="Medium"),
    MetricDimension(key="first_user_channel_group", name="Channel"),
    MetricDimension(key="first_user_campaign", name="Campaign"),
]
CONVERSION_DIMS = [
    MetricDimension(key="event_name", name="Event Name"),
    MetricDimension(key="session_source", name="Source"),
    MetricDimension(key="session_medium", name="Medium"),
    MetricDimension(key="session_channel_group", name="Channel"),
]


class _SimpleSumMetric(Metric):
    _table_key: str = ""
    _value_field: str = ""
    _time_column: str = "date"

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table(self._table_key), self._time_column, days, date)
        table = _apply_filters(table, filters)
        if dimensions:
            grouped = table.group_by(*dimensions)
            return [MetricResult(value=v, groups=parse_group_key(k, dimensions))
                    for k, v in sorted(grouped.aggregate(self._value_field, aggregation).items())]
        return [MetricResult(value=table.aggregate(self._value_field, aggregation))]


def _make_sum(metric_id: str, metric_name: str, desc: str, table_key: str,
              value_field: str, dims: list[MetricDimension], dt: MetricDataType = MetricDataType.NUMBER) -> _SimpleSumMetric:
    m = _SimpleSumMetric()
    m.id = metric_id
    m.name = metric_name
    m.description = desc
    m.data_type = dt
    m.source_type = DataSourceType.GOOGLE_ANALYTICS
    m.table_keys = [table_key]
    m.dimensions = dims
    m._table_key = table_key
    m._value_field = value_field
    return m


# --- Sessions & Traffic (simple sums) ---

register(_make_sum("builtin:ga.sessions", "Sessions", "Total number of sessions",
                             "google_analytics:traffic_sources", "sessions", TRAFFIC_DIMS))
register(_make_sum("builtin:ga.total_users", "Total Users", "Total number of unique users",
                             "google_analytics:traffic_sources", "total_users", TRAFFIC_DIMS))
register(_make_sum("builtin:ga.new_users", "New Users", "Number of first-time users",
                             "google_analytics:traffic_sources", "new_users", TRAFFIC_DIMS))
register(_make_sum("builtin:ga.page_views", "Page Views", "Total number of page views",
                             "google_analytics:traffic_sources", "page_views", TRAFFIC_DIMS))
register(_make_sum("builtin:ga.conversions", "Conversions", "Total number of conversion events",
                             "google_analytics:traffic_sources", "conversions", TRAFFIC_DIMS))
register(_make_sum("builtin:ga.revenue", "Revenue", "Total revenue from analytics events",
                             "google_analytics:traffic_sources", "total_revenue", TRAFFIC_DIMS))


# --- Sessions & Traffic (weighted averages / ratios) ---

class BounceRateMetric(Metric):
    id = "builtin:ga.bounce_rate"
    name = "Bounce Rate"
    description = "Weighted average bounce rate across sessions"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:traffic_sources"]
    dimensions = TRAFFIC_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:traffic_sources"), "date", days, date)
        table = _apply_filters(table, filters)
        rows = table.to_rows()
        total_sessions = sum(float(r.get("sessions", 0) or 0) for r in rows)
        if total_sessions == 0:
            return [MetricResult(value=0.0)]
        weighted = sum(float(r.get("bounce_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows)
        return [MetricResult(value=weighted / total_sessions)]


class EngagementRateMetric(Metric):
    id = "builtin:ga.engagement_rate"
    name = "Engagement Rate"
    description = "Weighted average engagement rate across sessions"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:traffic_sources"]
    dimensions = TRAFFIC_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:traffic_sources"), "date", days, date)
        table = _apply_filters(table, filters)
        rows = table.to_rows()
        total_sessions = sum(float(r.get("sessions", 0) or 0) for r in rows)
        if total_sessions == 0:
            return [MetricResult(value=0.0)]
        weighted = sum(float(r.get("engagement_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows)
        return [MetricResult(value=weighted / total_sessions)]


class AvgSessionDurationMetric(Metric):
    id = "builtin:ga.avg_session_duration"
    name = "Avg Session Duration"
    description = "Weighted average session duration in seconds"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:traffic_sources"]
    dimensions = TRAFFIC_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:traffic_sources"), "date", days, date)
        table = _apply_filters(table, filters)
        rows = table.to_rows()
        total_sessions = sum(float(r.get("sessions", 0) or 0) for r in rows)
        if total_sessions == 0:
            return [MetricResult(value=0.0)]
        weighted = sum(float(r.get("avg_session_duration", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows)
        return [MetricResult(value=weighted / total_sessions)]


class ConversionRateMetric(Metric):
    id = "builtin:ga.conversion_rate"
    name = "Conversion Rate"
    description = "Percentage of sessions that resulted in a conversion"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:traffic_sources"]
    dimensions = TRAFFIC_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:traffic_sources"), "date", days, date)
        table = _apply_filters(table, filters)
        conversions = table.aggregate("conversions", Aggregation.SUM)
        sessions = table.aggregate("sessions", Aggregation.SUM)
        value = (conversions / sessions * 100) if sessions > 0 else 0.0
        return [MetricResult(value=value)]


register(BounceRateMetric())
register(EngagementRateMetric())
register(AvgSessionDurationMetric())
register(ConversionRateMetric())


# --- Pages ---

register(_make_sum("builtin:ga.page_views_by_page", "Page Views by Page",
                             "Page views broken down by page path", "google_analytics:pages", "page_views", PAGE_DIMS))


class PageBounceRateMetric(Metric):
    id = "builtin:ga.page_bounce_rate"
    name = "Page Bounce Rate"
    description = "Bounce rate broken down by page"
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:pages"]
    dimensions = PAGE_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:pages"), "date", days, date)
        table = _apply_filters(table, filters)
        rows = table.to_rows()
        total_sessions = sum(float(r.get("sessions", 0) or 0) for r in rows)
        if total_sessions == 0:
            return [MetricResult(value=0.0)]
        weighted = sum(float(r.get("bounce_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows)
        return [MetricResult(value=weighted / total_sessions)]


register(PageBounceRateMetric())

# --- Events ---

register(_make_sum("builtin:ga.event_count", "Event Count", "Total number of events triggered",
                             "google_analytics:events", "event_count", EVENT_DIMS))
register(_make_sum("builtin:ga.event_value", "Event Value", "Total value of all events",
                             "google_analytics:events", "event_value", EVENT_DIMS))

# --- Geography ---

register(_make_sum("builtin:ga.sessions_by_country", "Sessions by Country",
                             "Sessions broken down by user country", "google_analytics:geography", "sessions", GEO_DIMS))

# --- Devices ---

register(_make_sum("builtin:ga.sessions_by_device", "Sessions by Device",
                             "Sessions broken down by device type", "google_analytics:devices", "sessions", DEVICE_DIMS))

# --- Users ---

register(_make_sum("builtin:ga.users_by_type", "Users by Type",
                             "Users broken down by new vs returning", "google_analytics:users", "total_users", USER_DIMS))

# --- User Acquisition ---

register(_make_sum("builtin:ga.new_users_by_channel", "New Users by Channel",
                             "New users by first-touch acquisition channel", "google_analytics:user_acquisition", "new_users",
                             ACQUISITION_DIMS))

# --- Conversions ---

register(_make_sum("builtin:ga.conversions_by_event", "Conversions by Event",
                             "Conversion count by event and traffic source", "google_analytics:conversions", "conversions",
                             CONVERSION_DIMS))


class RevenuePerConversionMetric(Metric):
    id = "builtin:ga.revenue_per_conversion"
    name = "Revenue per Conversion"
    description = "Average revenue generated per conversion event"
    data_type = MetricDataType.NUMBER
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = ["google_analytics:conversions"]
    dimensions = CONVERSION_DIMS

    def compute_single(self, ctx, dimensions=None, aggregation=Aggregation.SUM, filters=None, days=30, date=None):
        table = apply_time_filter(ctx.table("google_analytics:conversions"), "date", days, date)
        table = _apply_filters(table, filters)
        revenue = table.aggregate("total_revenue", Aggregation.SUM)
        conversions = table.aggregate("conversions", Aggregation.SUM)
        value = (revenue / conversions) if conversions > 0 else 0.0
        return [MetricResult(value=value)]


register(RevenuePerConversionMetric())


class SignupRateMetric(Metric):
    id = "builtin:ga.signup_rate"
    name = "Signup Rate"
    description = (
        "Percentage of sessions that resulted in a sign_up event"
    )
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = [
        "google_analytics:conversions",
        "google_analytics:traffic_sources",
    ]
    dimensions = CONVERSION_DIMS

    def compute_single(
        self, ctx, dimensions=None,
        aggregation=Aggregation.SUM, filters=None,
        days=30, date=None,
    ):
        conv_table = apply_time_filter(
            ctx.table("google_analytics:conversions"),
            "date", days, date,
        )
        conv_table = conv_table.filter(
            "event_name", "eq", "sign_up",
        )
        conv_table = _apply_filters(conv_table, filters)
        signups = conv_table.aggregate(
            "conversions", Aggregation.SUM,
        )

        traffic_table = apply_time_filter(
            ctx.table("google_analytics:traffic_sources"),
            "date", days, date,
        )
        sessions = traffic_table.aggregate(
            "sessions", Aggregation.SUM,
        )

        value = (signups / sessions * 100) if sessions > 0 else 0.0
        return [MetricResult(value=value)]


register(SignupRateMetric())


class SignupToPaidRateMetric(Metric):
    id = "builtin:ga.signup_to_paid_rate"
    name = "Signup-to-Paid Rate"
    description = (
        "Percentage of sign_up events that resulted in a "
        "purchase event within the same period"
    )
    data_type = MetricDataType.PERCENT
    source_type = DataSourceType.GOOGLE_ANALYTICS
    default_aggregation = Aggregation.AVG_DAILY
    table_keys = [
        "google_analytics:conversions",
    ]
    dimensions = []

    def compute_single(
        self, ctx, dimensions=None,
        aggregation=Aggregation.SUM, filters=None,
        days=30, date=None,
    ):
        conv_table = apply_time_filter(
            ctx.table("google_analytics:conversions"),
            "date", days, date,
        )

        signup_table = conv_table.filter(
            "event_name", "eq", "sign_up",
        )
        signup_table = _apply_filters(signup_table, filters)
        signups = signup_table.aggregate(
            "conversions", Aggregation.SUM,
        )

        purchase_table = conv_table.filter(
            "event_name", "eq", "purchase",
        )
        purchase_table = _apply_filters(purchase_table, filters)
        purchases = purchase_table.aggregate(
            "conversions", Aggregation.SUM,
        )

        value = (
            (purchases / signups * 100)
            if signups > 0 else 0.0
        )
        return [MetricResult(value=value)]


register(SignupToPaidRateMetric())
