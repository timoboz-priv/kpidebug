from kpidebug.data.types import Row
from kpidebug.metrics.builtin_metrics import (
    BuiltinMetric,
    MetricDimension,
    builtin_registry,
)


def _sum_field(rows: list[Row], field: str) -> float:
    return sum(float(r.get(field, 0) or 0) for r in rows)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


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

USER_DIMS = [
    MetricDimension(key="new_vs_returning", name="New vs Returning"),
]

PAGE_DIMS = [
    MetricDimension(key="page_path", name="Page Path"),
    MetricDimension(key="page_title", name="Page Title"),
]

EVENT_DIMS = [
    MetricDimension(key="event_name", name="Event Name"),
]

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

# --- Sessions & Traffic ---

builtin_registry.register(BuiltinMetric(
    key="ga.sessions",
    name="Sessions",
    description="Total number of sessions",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="sessions",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.total_users",
    name="Total Users",
    description="Total number of unique users",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="total_users",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.new_users",
    name="New Users",
    description="Number of first-time users",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="new_users",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.page_views",
    name="Page Views",
    description="Total number of page views",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="page_views",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.bounce_rate",
    name="Bounce Rate",
    description="Weighted average bounce rate across sessions",
    table="traffic_sources",
    time_column="date",
    data_type="percent",
    value_field="bounce_rate",
    dimensions=TRAFFIC_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        sum(float(r.get("bounce_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows),
        _sum_field(rows, "sessions"),
    ) if rows else 0.0,
))

builtin_registry.register(BuiltinMetric(
    key="ga.engagement_rate",
    name="Engagement Rate",
    description="Weighted average engagement rate across sessions",
    table="traffic_sources",
    time_column="date",
    data_type="percent",
    value_field="engagement_rate",
    dimensions=TRAFFIC_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        sum(float(r.get("engagement_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows),
        _sum_field(rows, "sessions"),
    ) if rows else 0.0,
))

builtin_registry.register(BuiltinMetric(
    key="ga.avg_session_duration",
    name="Avg Session Duration",
    description="Weighted average session duration in seconds",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="avg_session_duration",
    dimensions=TRAFFIC_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        sum(float(r.get("avg_session_duration", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows),
        _sum_field(rows, "sessions"),
    ) if rows else 0.0,
))

builtin_registry.register(BuiltinMetric(
    key="ga.conversions",
    name="Conversions",
    description="Total number of conversion events",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="conversions",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.revenue",
    name="Revenue",
    description="Total revenue from analytics events",
    table="traffic_sources",
    time_column="date",
    data_type="number",
    value_field="total_revenue",
    dimensions=TRAFFIC_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.conversion_rate",
    name="Conversion Rate",
    description="Percentage of sessions that resulted in a conversion",
    table="traffic_sources",
    time_column="date",
    data_type="percent",
    value_field="conversions",
    dimensions=TRAFFIC_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        _sum_field(rows, "conversions"),
        _sum_field(rows, "sessions"),
    ) * 100,
))

# --- Pages ---

builtin_registry.register(BuiltinMetric(
    key="ga.page_views_by_page",
    name="Page Views by Page",
    description="Page views broken down by page path",
    table="pages",
    time_column="date",
    data_type="number",
    value_field="page_views",
    dimensions=PAGE_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.page_bounce_rate",
    name="Page Bounce Rate",
    description="Bounce rate broken down by page",
    table="pages",
    time_column="date",
    data_type="percent",
    value_field="bounce_rate",
    dimensions=PAGE_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        sum(float(r.get("bounce_rate", 0) or 0) * float(r.get("sessions", 0) or 0) for r in rows),
        _sum_field(rows, "sessions"),
    ) if rows else 0.0,
))

# --- Events ---

builtin_registry.register(BuiltinMetric(
    key="ga.event_count",
    name="Event Count",
    description="Total number of events triggered",
    table="events",
    time_column="date",
    data_type="number",
    value_field="event_count",
    dimensions=EVENT_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.event_value",
    name="Event Value",
    description="Total value of all events",
    table="events",
    time_column="date",
    data_type="number",
    value_field="event_value",
    dimensions=EVENT_DIMS,
))

# --- Geography ---

builtin_registry.register(BuiltinMetric(
    key="ga.sessions_by_country",
    name="Sessions by Country",
    description="Sessions broken down by user country",
    table="geography",
    time_column="date",
    data_type="number",
    value_field="sessions",
    dimensions=GEO_DIMS,
))

# --- Devices ---

builtin_registry.register(BuiltinMetric(
    key="ga.sessions_by_device",
    name="Sessions by Device",
    description="Sessions broken down by device type",
    table="devices",
    time_column="date",
    data_type="number",
    value_field="sessions",
    dimensions=DEVICE_DIMS,
))

# --- Users ---

builtin_registry.register(BuiltinMetric(
    key="ga.users_by_type",
    name="Users by Type",
    description="Users broken down by new vs returning",
    table="users",
    time_column="date",
    data_type="number",
    value_field="total_users",
    dimensions=USER_DIMS,
))

# --- User Acquisition ---

builtin_registry.register(BuiltinMetric(
    key="ga.new_users_by_channel",
    name="New Users by Channel",
    description="New users broken down by first-touch acquisition channel",
    table="user_acquisition",
    time_column="date",
    data_type="number",
    value_field="new_users",
    dimensions=ACQUISITION_DIMS,
))

# --- Conversions ---

builtin_registry.register(BuiltinMetric(
    key="ga.conversions_by_event",
    name="Conversions by Event",
    description="Conversion count broken down by event and traffic source",
    table="conversions",
    time_column="date",
    data_type="number",
    value_field="conversions",
    dimensions=CONVERSION_DIMS,
))

builtin_registry.register(BuiltinMetric(
    key="ga.revenue_per_conversion",
    name="Revenue per Conversion",
    description="Average revenue generated per conversion event",
    table="conversions",
    time_column="date",
    data_type="number",
    value_field="total_revenue",
    dimensions=CONVERSION_DIMS,
    compute_fn=lambda rows: _safe_ratio(
        _sum_field(rows, "total_revenue"),
        _sum_field(rows, "conversions"),
    ),
))
