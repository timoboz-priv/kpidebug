import uuid
from datetime import datetime, timezone

from google.cloud.firestore import Client as FirestoreClient

from kpidebug.data.types import DataSourceType, DimensionValue
from kpidebug.metrics.types import (
    MetricDataType,
    MetricDefinition,
    MetricResult,
    MetricSource,
    SourceFilter,
)
from kpidebug.metrics.metric_store import AbstractMetricStore


class FirestoreMetricStore(AbstractMetricStore):
    DEFINITIONS_COLLECTION = "metric_definitions"
    RESULTS_COLLECTION = "metric_results"

    def __init__(self, db: FirestoreClient):
        self.db = db

    def _project_ref(self, project_id: str):
        return self.db.collection("projects").document(project_id)

    def create_definition(self, definition: MetricDefinition) -> MetricDefinition:
        metric_id = definition.id or str(uuid.uuid4())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        self._project_ref(definition.project_id).collection(
            self.DEFINITIONS_COLLECTION
        ).document(metric_id).set({
            "name": definition.name,
            "description": definition.description,
            "data_type": definition.data_type.value,
            "source": definition.source.value,
            "builtin_key": definition.builtin_key,
            "computation": definition.computation,
            "source_filters": [
                {"source_type": sf.source_type.value, "fields": sf.fields}
                for sf in definition.source_filters
            ],
            "dimensions": definition.dimensions,
            "created_at": now,
            "updated_at": now,
        })

        definition.id = metric_id
        definition.created_at = now
        definition.updated_at = now
        return definition

    def get_definition(self, project_id: str, metric_id: str) -> MetricDefinition | None:
        doc = (
            self._project_ref(project_id)
            .collection(self.DEFINITIONS_COLLECTION)
            .document(metric_id)
            .get()
        )
        if not doc.exists:
            return None
        return self._doc_to_definition(doc, project_id)

    def list_definitions(self, project_id: str) -> list[MetricDefinition]:
        docs = self._project_ref(project_id).collection(self.DEFINITIONS_COLLECTION).get()
        return [self._doc_to_definition(doc, project_id) for doc in docs]

    def update_definition(self, project_id: str, metric_id: str, updates: dict) -> MetricDefinition:
        updates["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._project_ref(project_id).collection(
            self.DEFINITIONS_COLLECTION
        ).document(metric_id).update(updates)
        return self.get_definition(project_id, metric_id)

    def delete_definition(self, project_id: str, metric_id: str) -> None:
        self._project_ref(project_id).collection(
            self.DEFINITIONS_COLLECTION
        ).document(metric_id).delete()

    def store_results(self, results: list[MetricResult]) -> None:
        if not results:
            return

        batch = self.db.batch()
        project_id = results[0].project_id
        collection = self._project_ref(project_id).collection(self.RESULTS_COLLECTION)

        for result in results:
            result_id = result.id or str(uuid.uuid4())
            doc_ref = collection.document(result_id)
            batch.set(doc_ref, {
                "metric_id": result.metric_id,
                "value": result.value,
                "dimension_values": [
                    {"dimension": dv.dimension, "value": dv.value}
                    for dv in result.dimension_values
                ],
                "computed_at": result.computed_at,
                "period_start": result.period_start,
                "period_end": result.period_end,
            })

        batch.commit()

    def get_results(
        self,
        project_id: str,
        metric_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[MetricResult]:
        query = (
            self._project_ref(project_id)
            .collection(self.RESULTS_COLLECTION)
            .where("metric_id", "==", metric_id)
        )

        if start_time is not None:
            query = query.where("computed_at", ">=", start_time)
        if end_time is not None:
            query = query.where("computed_at", "<=", end_time)

        docs = query.get()
        return [self._doc_to_result(doc, project_id) for doc in docs]

    def get_latest_result(self, project_id: str, metric_id: str) -> MetricResult | None:
        docs = (
            self._project_ref(project_id)
            .collection(self.RESULTS_COLLECTION)
            .where("metric_id", "==", metric_id)
            .order_by("computed_at", direction="DESCENDING")
            .limit(1)
            .get()
        )

        for doc in docs:
            return self._doc_to_result(doc, project_id)
        return None

    def _doc_to_definition(self, doc, project_id: str) -> MetricDefinition:
        data = doc.to_dict()
        source_filters = [
            SourceFilter(
                source_type=DataSourceType(sf["source_type"]),
                fields=sf.get("fields", []),
            )
            for sf in data.get("source_filters", [])
        ]
        return MetricDefinition(
            id=doc.id,
            project_id=project_id,
            name=data.get("name", ""),
            description=data.get("description", ""),
            data_type=MetricDataType(data.get("data_type", MetricDataType.NUMBER)),
            source=MetricSource(data.get("source", MetricSource.BUILTIN)),
            builtin_key=data.get("builtin_key", ""),
            computation=data.get("computation", ""),
            source_filters=source_filters,
            dimensions=data.get("dimensions", []),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
        )

    def _doc_to_result(self, doc, project_id: str) -> MetricResult:
        data = doc.to_dict()
        dimension_values = [
            DimensionValue(dimension=dv["dimension"], value=dv["value"])
            for dv in data.get("dimension_values", [])
        ]
        return MetricResult(
            id=doc.id,
            project_id=project_id,
            metric_id=data.get("metric_id", ""),
            value=float(data.get("value", 0.0)),
            dimension_values=dimension_values,
            computed_at=data.get("computed_at", ""),
            period_start=data.get("period_start", ""),
            period_end=data.get("period_end", ""),
        )
