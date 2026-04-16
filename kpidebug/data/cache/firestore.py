import json

from google.cloud.firestore import Client as FirestoreClient

from kpidebug.data.cache.base import TableCache


class FirestoreTableCache(TableCache):
    COLLECTION = "table_cache"

    def __init__(self, db: FirestoreClient):
        self.db = db

    def _table_ref(self, source_id: str, table_key: str):
        return (
            self.db.collection(self.COLLECTION)
            .document(source_id)
            .collection(table_key)
        )

    def get_rows(
        self, source_id: str, table_key: str,
    ) -> list[dict] | None:
        if not self.is_cached(source_id, table_key):
            return None
        docs = self._table_ref(source_id, table_key).get()
        return [json.loads(doc.to_dict()["data"]) for doc in docs]

    def set_rows(
        self, source_id: str, table_key: str,
        rows: list[dict],
    ) -> None:
        self.clear_table(source_id, table_key)
        ref = self._table_ref(source_id, table_key)
        for i, row in enumerate(rows):
            ref.document(str(i)).set(
                {"data": json.dumps(row)}
            )
        # Mark as cached
        self.db.collection(self.COLLECTION).document(
            source_id
        ).set(
            {f"_cached_{table_key}": True}, merge=True
        )

    def sync_rows(
        self, source_id: str, table_key: str,
        fresh_rows: list[dict],
        pk_columns: list[str],
    ) -> None:
        if not pk_columns or not self.is_cached(
            source_id, table_key
        ):
            self.set_rows(source_id, table_key, fresh_rows)
            return

        ref = self._table_ref(source_id, table_key)

        # Build PK index of existing cached rows
        existing_docs = ref.get()
        cached_by_pk: dict[str, str] = {}
        cached_data: dict[str, dict] = {}
        for doc in existing_docs:
            row = json.loads(doc.to_dict()["data"])
            pk = _pk_value(row, pk_columns)
            cached_by_pk[pk] = doc.id
            cached_data[pk] = row

        # Build PK index of fresh rows
        fresh_by_pk: dict[str, dict] = {}
        for row in fresh_rows:
            pk = _pk_value(row, pk_columns)
            fresh_by_pk[pk] = row

        # Delete rows no longer in source
        for pk, doc_id in cached_by_pk.items():
            if pk not in fresh_by_pk:
                ref.document(doc_id).delete()

        # Upsert new/changed rows
        for pk, row in fresh_by_pk.items():
            if pk in cached_by_pk:
                if cached_data[pk] != row:
                    doc_id = cached_by_pk[pk]
                    ref.document(doc_id).set(
                        {"data": json.dumps(row)}
                    )
            else:
                ref.add({"data": json.dumps(row)})

    def is_cached(
        self, source_id: str, table_key: str,
    ) -> bool:
        doc = self.db.collection(self.COLLECTION).document(
            source_id
        ).get()
        if not doc.exists:
            return False
        return doc.to_dict().get(
            f"_cached_{table_key}", False
        )

    def clear_table(
        self, source_id: str, table_key: str,
    ) -> None:
        ref = self._table_ref(source_id, table_key)
        docs = ref.get()
        for doc in docs:
            doc.reference.delete()
        # Remove cached marker
        self.db.collection(self.COLLECTION).document(
            source_id
        ).set(
            {f"_cached_{table_key}": False}, merge=True
        )


def _pk_value(
    row: dict, pk_columns: list[str],
) -> str:
    return "|".join(str(row.get(c, "")) for c in pk_columns)
