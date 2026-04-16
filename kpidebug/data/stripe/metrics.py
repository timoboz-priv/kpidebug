from kpidebug.data.connector import MetricDescriptor
from kpidebug.data.types import Dimension, DimensionType

TIME_DIMENSION = Dimension(name="time", type=DimensionType.TEMPORAL)
CURRENCY_DIMENSION = Dimension(
    name="currency", type=DimensionType.CATEGORICAL
)
CHARGE_STATUS_DIMENSION = Dimension(
    name="charge_status", type=DimensionType.CATEGORICAL
)
SUBSCRIPTION_STATUS_DIMENSION = Dimension(
    name="subscription_status", type=DimensionType.CATEGORICAL
)
SUBSCRIPTION_INTERVAL_DIMENSION = Dimension(
    name="subscription_interval", type=DimensionType.CATEGORICAL
)
INVOICE_STATUS_DIMENSION = Dimension(
    name="invoice_status", type=DimensionType.CATEGORICAL
)

STRIPE_METRICS: list[MetricDescriptor] = [
    # Charges
    MetricDescriptor(
        key="charges.amount",
        name="Charge Amount",
        description="Amount charged (in minor units, e.g. cents)",
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            CHARGE_STATUS_DIMENSION,
        ],
    ),
    MetricDescriptor(
        key="charges.count",
        name="Charge Count",
        description="Number of charges",
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            CHARGE_STATUS_DIMENSION,
        ],
    ),
    # Customers
    MetricDescriptor(
        key="customers.count",
        name="Customer Count",
        description="Number of customers",
        dimensions=[TIME_DIMENSION],
    ),
    # Subscriptions
    MetricDescriptor(
        key="subscriptions.count",
        name="Subscription Count",
        description="Number of subscriptions",
        dimensions=[
            TIME_DIMENSION,
            SUBSCRIPTION_STATUS_DIMENSION,
            SUBSCRIPTION_INTERVAL_DIMENSION,
        ],
    ),
    MetricDescriptor(
        key="subscriptions.amount",
        name="Subscription Amount",
        description=(
            "Recurring amount per subscription "
            "(in minor units, e.g. cents)"
        ),
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            SUBSCRIPTION_STATUS_DIMENSION,
            SUBSCRIPTION_INTERVAL_DIMENSION,
        ],
    ),
    # Invoices
    MetricDescriptor(
        key="invoices.amount_due",
        name="Invoice Amount Due",
        description="Total amount due on invoices (minor units)",
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            INVOICE_STATUS_DIMENSION,
        ],
    ),
    MetricDescriptor(
        key="invoices.amount_paid",
        name="Invoice Amount Paid",
        description="Total amount paid on invoices (minor units)",
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            INVOICE_STATUS_DIMENSION,
        ],
    ),
    MetricDescriptor(
        key="invoices.count",
        name="Invoice Count",
        description="Number of invoices",
        dimensions=[
            TIME_DIMENSION,
            CURRENCY_DIMENSION,
            INVOICE_STATUS_DIMENSION,
        ],
    ),
    # Refunds
    MetricDescriptor(
        key="refunds.amount",
        name="Refund Amount",
        description="Total refund amount (minor units)",
        dimensions=[TIME_DIMENSION, CURRENCY_DIMENSION],
    ),
    MetricDescriptor(
        key="refunds.count",
        name="Refund Count",
        description="Number of refunds",
        dimensions=[TIME_DIMENSION, CURRENCY_DIMENSION],
    ),
]

STRIPE_METRICS_BY_KEY: dict[str, MetricDescriptor] = {
    m.key: m for m in STRIPE_METRICS
}

METRIC_RESOURCE_MAP: dict[str, str] = {
    "charges": "charges",
    "customers": "customers",
    "subscriptions": "subscriptions",
    "invoices": "invoices",
    "refunds": "refunds",
}
