from dataclasses import dataclass

import firebase_admin
from firebase_admin import auth as firebase_auth, credentials
from fastapi import Depends, Header, HTTPException
from google.cloud.firestore import Client as FirestoreClient

from kpidebug.config import config
from kpidebug.management.types import Project, ProjectMember, Role, User
from kpidebug.management.user_store import AbstractUserStore
from kpidebug.management.user_store_firestore import FirestoreUserStore
from kpidebug.management.project_store import AbstractProjectStore
from kpidebug.management.project_store_firestore import FirestoreProjectStore
from kpidebug.data.data_store import AbstractDataStore
from kpidebug.data.data_store_firestore import FirestoreDataStore
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.metric_store_firestore import FirestoreMetricStore

_firebase_app: firebase_admin.App | None = None


def _get_firebase_app() -> firebase_admin.App:
    global _firebase_app
    if _firebase_app is None:
        cred = credentials.Certificate(config.google_application_credentials)
        _firebase_app = firebase_admin.initialize_app(cred)
    return _firebase_app


@dataclass
class FirebaseUser:
    uid: str
    email: str | None
    name: str | None
    picture: str | None


def verify_firebase_token(authorization: str = Header(...)) -> FirebaseUser:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[len("Bearer "):]

    try:
        _get_firebase_app()
        decoded = firebase_auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    name = decoded.get("name")
    picture = decoded.get("picture")

    if not name or not picture:
        fb_user = firebase_auth.get_user(decoded["uid"])
        name = name or fb_user.display_name
        picture = picture or fb_user.photo_url

    return FirebaseUser(
        uid=decoded["uid"],
        email=decoded.get("email"),
        name=name,
        picture=picture,
    )


_firestore_client: FirestoreClient | None = None


def get_firestore_client() -> FirestoreClient:
    global _firestore_client
    if _firestore_client is None:
        _firestore_client = FirestoreClient()
    return _firestore_client


def get_user_store() -> AbstractUserStore:
    return FirestoreUserStore(get_firestore_client())


def get_project_store() -> AbstractProjectStore:
    return FirestoreProjectStore(get_firestore_client())


def get_data_store() -> AbstractDataStore:
    return FirestoreDataStore(get_firestore_client())


def get_metric_store() -> AbstractMetricStore:
    return FirestoreMetricStore(get_firestore_client())


def get_table_cache():
    from kpidebug.config import config
    from kpidebug.data.cache.base import TableCache

    if not config.cache_enabled:
        return None

    if config.cache_backend == "firestore":
        from kpidebug.data.cache.firestore import (
            FirestoreTableCache,
        )
        return FirestoreTableCache(get_firestore_client())
    else:
        return _get_memory_cache()


_memory_cache_instance = None


def _get_memory_cache():
    global _memory_cache_instance
    if _memory_cache_instance is None:
        from kpidebug.data.cache.memory import (
            InMemoryTableCache,
        )
        _memory_cache_instance = InMemoryTableCache()
    return _memory_cache_instance


def get_current_user(
    firebase_user: FirebaseUser = Depends(verify_firebase_token),
    user_store: AbstractUserStore = Depends(get_user_store),
) -> User:
    user = user_store.get_or_create(
        user_id=firebase_user.uid,
        email=firebase_user.email,
        name=firebase_user.name,
        avatar_url=firebase_user.picture,
    )
    return user


def get_current_project(
    current_user: User = Depends(get_current_user),
    project_store: AbstractProjectStore = Depends(get_project_store),
    x_project_id: str | None = Header(None),
) -> Project | None:
    if x_project_id is None:
        return None
    project = project_store.get(x_project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    member = project_store.get_member(x_project_id, current_user.id)
    if member is None:
        raise HTTPException(status_code=403, detail="Not a member of this project")
    return project


def require_project_role(required_role: Role):
    def dependency(
        current_user: User = Depends(get_current_user),
        project_store: AbstractProjectStore = Depends(get_project_store),
        x_project_id: str = Header(...),
    ) -> ProjectMember:
        member = project_store.get_member(x_project_id, current_user.id)
        if member is None:
            raise HTTPException(status_code=403, detail="Not a member of this project")

        role_hierarchy = {Role.READ: 0, Role.EDIT: 1, Role.ADMIN: 2}
        if role_hierarchy[member.role] < role_hierarchy[required_role]:
            raise HTTPException(status_code=403, detail=f"Requires {required_role.value} role")

        return member
    return dependency
