from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.user_store import AbstractUserStore
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store import AbstractProjectStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.management.artifact_store import AbstractArtifactStore
from kpidebug.management.artifact_store_postgres import PostgresArtifactStore
from kpidebug.data.data_source_store import AbstractDataSourceStore
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore


def _get_pool_manager() -> ConnectionPoolManager:
    return ConnectionPoolManager.get_instance()


def get_user_store() -> AbstractUserStore:
    return PostgresUserStore(_get_pool_manager())


def get_project_store() -> AbstractProjectStore:
    return PostgresProjectStore(_get_pool_manager())


def get_data_source_store() -> AbstractDataSourceStore:
    return PostgresDataSourceStore(_get_pool_manager())


def get_metric_store() -> AbstractMetricStore:
    return PostgresMetricStore(_get_pool_manager())


def get_dashboard_store() -> AbstractDashboardStore:
    return PostgresDashboardStore(_get_pool_manager())


def get_artifact_store() -> AbstractArtifactStore:
    return PostgresArtifactStore(_get_pool_manager())
