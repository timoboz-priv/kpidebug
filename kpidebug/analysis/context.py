from __future__ import annotations

import logging
from datetime import date

from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.table import DataTable
from kpidebug.data.types import TableDescriptor
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.expression_metric import ExpressionMetric
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.types import DashboardMetric, Metric
import kpidebug.metrics.registry as registry

logger = logging.getLogger(__name__)


class AnalysisContext:
    _project_id: str
    _dashboard_metrics: list[DashboardMetric]
    _metric_context: MetricContext
    _metric_store: AbstractMetricStore
    _data_source_store: PostgresDataSourceStore
    _as_of_date: date | None

    _resolved_metrics: dict[str, Metric | None]
    _resolved_tables: dict[str, DataTable]
    _table_descriptors: list[TableDescriptor] | None

    def __init__(
        self,
        project_id: str,
        dashboard_metrics: list[DashboardMetric],
        metric_context: MetricContext,
        metric_store: AbstractMetricStore,
        data_source_store: PostgresDataSourceStore,
        as_of_date: date | None = None,
    ):
        self._project_id = project_id
        self._dashboard_metrics = dashboard_metrics
        self._metric_context = metric_context
        self._metric_store = metric_store
        self._data_source_store = data_source_store
        self._as_of_date = as_of_date
        self._resolved_metrics = {}
        self._resolved_tables = {}
        self._table_descriptors = None

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def as_of_date(self) -> date | None:
        return self._as_of_date

    @property
    def dashboard_metrics(self) -> list[DashboardMetric]:
        return self._dashboard_metrics

    def get_dashboard_metric(self, metric_id: str) -> DashboardMetric | None:
        for dm in self._dashboard_metrics:
            if dm.metric_id == metric_id:
                return dm
        return None

    def get_metric(self, metric_id: str) -> Metric | None:
        if metric_id in self._resolved_metrics:
            return self._resolved_metrics[metric_id]

        metric = self._resolve_metric(metric_id)
        self._resolved_metrics[metric_id] = metric
        return metric

    def list_metrics(self) -> list[Metric]:
        result: list[Metric] = []
        for dm in self._dashboard_metrics:
            metric = self.get_metric(dm.metric_id)
            if metric is not None:
                result.append(metric)
        return result

    def get_table(self, table_key: str) -> DataTable:
        if table_key in self._resolved_tables:
            return self._resolved_tables[table_key]

        table = self._metric_context.table(table_key)
        self._resolved_tables[table_key] = table
        return table

    def list_tables(self) -> list[TableDescriptor]:
        if self._table_descriptors is not None:
            return self._table_descriptors

        descriptors: list[TableDescriptor] = []
        for connector in self._metric_context._connectors:
            try:
                descriptors.extend(connector.get_tables())
            except Exception:
                logger.warning("Failed to list tables from connector for source %s", connector.source.id)
        self._table_descriptors = descriptors
        return descriptors

    def _resolve_metric(self, metric_id: str) -> Metric | None:
        builtin = registry.get(metric_id)
        if builtin is not None:
            return builtin

        definition = self._metric_store.get_definition(self._project_id, metric_id)
        if definition is not None:
            return ExpressionMetric(definition)

        return None
