import uuid

from google.cloud.firestore import Client as FirestoreClient

from kpidebug.data.types import (
    DataSource,
    DataSourceType,
    Dimension,
    DimensionType,
)
from kpidebug.data.data_store import AbstractDataStore


class FirestoreDataStore(AbstractDataStore):
    SOURCES_COLLECTION = "data_sources"
    CREDENTIALS_COLLECTION = "data_source_credentials"

    def __init__(self, db: FirestoreClient):
        self.db = db

    def _project_ref(self, project_id: str):
        return self.db.collection("projects").document(project_id)

    def create_source(
        self, project_id: str, name: str,
        source_type: DataSourceType,
        dimensions: list[Dimension],
    ) -> DataSource:
        source_id = str(uuid.uuid4())
        col = self._project_ref(project_id).collection(
            self.SOURCES_COLLECTION
        )
        col.document(source_id).set({
            "name": name,
            "type": source_type.value,
            "dimensions": [
                {"name": d.name, "type": d.type.value}
                for d in dimensions
            ],
        })
        return DataSource(
            id=source_id,
            project_id=project_id,
            name=name,
            type=source_type,
            dimensions=dimensions,
        )

    def get_source(
        self, project_id: str, source_id: str,
    ) -> DataSource | None:
        doc = (
            self._project_ref(project_id)
            .collection(self.SOURCES_COLLECTION)
            .document(source_id)
            .get()
        )
        if not doc.exists:
            return None
        return self._doc_to_source(doc, project_id)

    def list_sources(
        self, project_id: str,
    ) -> list[DataSource]:
        docs = (
            self._project_ref(project_id)
            .collection(self.SOURCES_COLLECTION)
            .get()
        )
        return [
            self._doc_to_source(doc, project_id)
            for doc in docs
        ]

    def delete_source(
        self, project_id: str, source_id: str,
    ) -> None:
        self._project_ref(project_id).collection(
            self.SOURCES_COLLECTION
        ).document(source_id).delete()

    def store_credentials(
        self, project_id: str, source_id: str,
        credentials: dict[str, str],
    ) -> None:
        self._project_ref(project_id).collection(
            self.CREDENTIALS_COLLECTION
        ).document(source_id).set(credentials)

    def get_credentials(
        self, project_id: str, source_id: str,
    ) -> dict[str, str] | None:
        doc = (
            self._project_ref(project_id)
            .collection(self.CREDENTIALS_COLLECTION)
            .document(source_id)
            .get()
        )
        if not doc.exists:
            return None
        return doc.to_dict()

    def delete_credentials(
        self, project_id: str, source_id: str,
    ) -> None:
        self._project_ref(project_id).collection(
            self.CREDENTIALS_COLLECTION
        ).document(source_id).delete()

    def _doc_to_source(
        self, doc, project_id: str,
    ) -> DataSource:
        data = doc.to_dict()
        dimensions = [
            Dimension(
                name=d["name"],
                type=DimensionType(d["type"]),
            )
            for d in data.get("dimensions", [])
        ]
        return DataSource(
            id=doc.id,
            project_id=project_id,
            name=data.get("name", ""),
            type=DataSourceType(
                data.get("type", DataSourceType.CUSTOM)
            ),
            dimensions=dimensions,
        )
