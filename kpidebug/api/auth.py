from dataclasses import dataclass

import firebase_admin
from firebase_admin import auth as firebase_auth, credentials
from fastapi import Depends, Header, HTTPException

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.config import config
from kpidebug.management.types import Project, ProjectMember, Role, User
from kpidebug.management.user_store import AbstractUserStore
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store import AbstractProjectStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.data.data_source_store import DataSourceStore
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore

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


def _get_pool_manager() -> ConnectionPoolManager:
    return ConnectionPoolManager.get_instance()


def get_user_store() -> AbstractUserStore:
    return PostgresUserStore(_get_pool_manager())


def get_project_store() -> AbstractProjectStore:
    return PostgresProjectStore(_get_pool_manager())


def get_data_source_store() -> DataSourceStore:
    return PostgresDataSourceStore(_get_pool_manager())


def get_metric_store() -> AbstractMetricStore:
    return PostgresMetricStore(_get_pool_manager())


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
