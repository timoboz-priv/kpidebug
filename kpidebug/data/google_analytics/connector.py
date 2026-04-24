import json
from typing import Callable

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from google.oauth2 import service_account

from kpidebug.data.connector import ConnectorError, DataSourceConnector
from kpidebug.data.types import (
    DataSource,
    TableDescriptor,
    TableFilter,
    TableQuery,
    TableResult,
)
from kpidebug.data.google_analytics.tables import (
    GA_DIMENSION_MAP,
    GA_METRIC_MAP,
    GA_TABLES,
    GA_TABLES_BY_KEY,
)


class GoogleAnalyticsConnector(DataSourceConnector):

    def __init__(self, source: DataSource):
        super().__init__(source)

    def validate_credentials(self) -> bool:
        client = self._make_client()
        property_id = self._get_property_id()
        try:
            client.run_report(RunReportRequest(
                property=f"properties/{property_id}",
                metrics=[Metric(name="sessions")],
                date_ranges=[DateRange(
                    start_date="1daysAgo", end_date="today",
                )],
            ))
            return True
        except Exception as e:
            raise ConnectorError(f"GA4 validation failed: {e}")

    def get_tables(self) -> list[TableDescriptor]:
        return list(GA_TABLES)

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
        table = GA_TABLES_BY_KEY.get(table_key)
        if table is None:
            raise ConnectorError(f"Unknown table: {table_key}")

        client = self._make_client()
        property_id = self._get_property_id()

        dim_cols = [
            c for c in table.columns
            if c.key in GA_DIMENSION_MAP
        ]
        metric_cols = [
            c for c in table.columns
            if c.key in GA_METRIC_MAP
        ]

        ga_dims = [
            Dimension(name=GA_DIMENSION_MAP[c.key])
            for c in dim_cols
        ]
        GA_METRICS_PER_REQUEST = 10
        metric_batches = [
            metric_cols[i:i + GA_METRICS_PER_REQUEST]
            for i in range(0, len(metric_cols), GA_METRICS_PER_REQUEST)
        ]

        all_rows: list[dict] = []

        for batch_idx, batch_cols in enumerate(metric_batches):
            ga_metrics = [
                Metric(name=GA_METRIC_MAP[c.key])
                for c in batch_cols
            ]

            offset = 0
            limit = 10000
            batch_row_idx = 0

            while True:
                request = RunReportRequest(
                    property=f"properties/{property_id}",
                    dimensions=ga_dims,
                    metrics=ga_metrics,
                    date_ranges=[DateRange(
                        start_date="365daysAgo",
                        end_date="today",
                    )],
                    limit=limit,
                    offset=offset,
                )

                response = client.run_report(request)

                for row in response.rows:
                    if batch_idx == 0:
                        record: dict = {}
                        for i, col in enumerate(dim_cols):
                            val = row.dimension_values[i].value
                            if col.key == "date":
                                val = (
                                    f"{val[:4]}-{val[4:6]}-{val[6:8]}"
                                    if len(val) == 8 else val
                                )
                            record[col.key] = val
                        all_rows.append(record)

                    target = all_rows[batch_row_idx] if batch_row_idx < len(all_rows) else {}

                    for i, col in enumerate(batch_cols):
                        raw = row.metric_values[i].value
                        try:
                            target[col.key] = float(raw)
                        except (ValueError, TypeError):
                            target[col.key] = 0.0

                    batch_row_idx += 1

                if len(response.rows) < limit:
                    break
                offset += limit

        return all_rows

    def _make_client(self) -> BetaAnalyticsDataClient:
        sa_json = self.source.credentials.get(
            "service_account_json", "",
        )
        if not sa_json:
            raise ConnectorError(
                "Missing service_account_json in credentials"
            )
        try:
            info = json.loads(sa_json)
        except json.JSONDecodeError:
            raise ConnectorError(
                "Invalid service account JSON"
            )
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/analytics.readonly",
            ],
        )
        return BetaAnalyticsDataClient(credentials=creds)

    def _get_property_id(self) -> str:
        prop_id = self.source.credentials.get(
            "property_id", "",
        )
        if not prop_id:
            raise ConnectorError(
                "Missing property_id in credentials"
            )
        return prop_id


def _apply_filters(
    rows: list[dict], filters: list[TableFilter],
) -> list[dict]:
    if not filters:
        return rows
    return [
        r for r in rows
        if all(_matches(r, f) for f in filters)
    ]


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
        try:
            fval, ftarget = float(val), float(target)
        except ValueError:
            return val > target if f.operator.startswith("g") else val < target
        if f.operator == "gt":
            return fval > ftarget
        elif f.operator == "gte":
            return fval >= ftarget
        elif f.operator == "lt":
            return fval < ftarget
        elif f.operator == "lte":
            return fval <= ftarget
    return True


def _apply_sort(
    rows: list[dict],
    sort_by: str | None,
    sort_order: str,
) -> list[dict]:
    if not sort_by:
        return rows
    return sorted(
        rows,
        key=lambda r: r.get(sort_by, ""),
        reverse=(sort_order == "desc"),
    )
