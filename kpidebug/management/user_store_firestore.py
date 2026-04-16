from google.cloud.firestore import Client as FirestoreClient

from kpidebug.management.types import User
from kpidebug.management.user_store import AbstractUserStore


class FirestoreUserStore(AbstractUserStore):
    COLLECTION = "users"

    def __init__(self, db: FirestoreClient):
        self.db = db

    def get(self, user_id: str) -> User | None:
        doc = self.db.collection(self.COLLECTION).document(user_id).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        return User(
            id=doc.id,
            name=data.get("name", ""),
            email=data.get("email", ""),
            avatar_url=data.get("avatar_url", ""),
        )

    def create(self, user: User) -> User:
        self.db.collection(self.COLLECTION).document(user.id).set({
            "name": user.name,
            "email": user.email,
            "avatar_url": user.avatar_url,
        })
        return user

    def update(self, user_id: str, updates: dict) -> User:
        doc_ref = self.db.collection(self.COLLECTION).document(user_id)
        doc_ref.update(updates)
        return self.get(user_id)

    def get_or_create(self, user_id: str, email: str | None, name: str | None, avatar_url: str | None) -> User:
        existing = self.get(user_id)
        if existing is not None:
            return existing
        user = User(
            id=user_id,
            name=name or "",
            email=email or "",
            avatar_url=avatar_url or "",
        )
        return self.create(user)
