from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import get_current_user, get_project_store, get_user_store, require_project_role
from kpidebug.management.types import (
    AddMemberRequest,
    Project,
    ProjectMember,
    Role,
    User,
)
from kpidebug.management.project_store import AbstractProjectStore
from kpidebug.management.user_store import AbstractUserStore

router = APIRouter(prefix="/api/projects", tags=["projects"])


@router.get("")
def list_projects(
    current_user: User = Depends(get_current_user),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> list[Project]:
    return project_store.list_for_user(current_user.id)


@router.post("")
def create_project(
    body: Project,
    current_user: User = Depends(get_current_user),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> Project:
    return project_store.create(
        name=body.name,
        description=body.description,
        creator_id=current_user.id,
        creator_name=current_user.name,
        creator_email=current_user.email,
    )


@router.get("/{project_id}")
def get_project(
    project_id: str,
    current_user: User = Depends(get_current_user),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> Project:
    _require_membership(project_store, project_id, current_user.id)
    project = project_store.get(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.put("/{project_id}")
def update_project(
    project_id: str,
    body: Project,
    _admin: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> Project:
    return project_store.update(project_id, {
        "name": body.name,
        "description": body.description,
    })


@router.delete("/{project_id}")
def delete_project(
    project_id: str,
    _admin: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> dict:
    project_store.delete(project_id)
    return {"ok": True}


@router.get("/{project_id}/members")
def list_members(
    project_id: str,
    current_user: User = Depends(get_current_user),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> list[ProjectMember]:
    _require_membership(project_store, project_id, current_user.id)
    return project_store.get_members(project_id)


@router.post("/{project_id}/members")
def add_member(
    project_id: str,
    body: AddMemberRequest,
    _admin: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    project_store: AbstractProjectStore = Depends(get_project_store),
    user_store: AbstractUserStore = Depends(get_user_store),
) -> ProjectMember:
    # Find user by email
    # For now, we create a placeholder user entry if they don't exist yet.
    # They'll be fully provisioned on first login.
    from google.cloud.firestore import Client as FirestoreClient
    db = project_store.db if hasattr(project_store, "db") else None
    user_id = _find_user_id_by_email(db, body.email)
    if user_id is None:
        raise HTTPException(status_code=404, detail="No user found with that email. They must sign in at least once first.")

    existing = project_store.get_member(project_id, user_id)
    if existing is not None:
        raise HTTPException(status_code=409, detail="User is already a member of this project")

    user = user_store.get(user_id)
    return project_store.add_member(
        project_id=project_id,
        user_id=user_id,
        role=body.role,
        user_name=user.name if user else "",
        user_email=body.email,
    )


@router.put("/{project_id}/members/{user_id}")
def update_member_role(
    project_id: str,
    user_id: str,
    body: ProjectMember,
    _admin: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> ProjectMember:
    member = project_store.get_member(project_id, user_id)
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    return project_store.update_member_role(project_id, user_id, body.role)


@router.delete("/{project_id}/members/{user_id}")
def remove_member(
    project_id: str,
    user_id: str,
    _admin: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    project_store: AbstractProjectStore = Depends(get_project_store),
) -> dict:
    member = project_store.get_member(project_id, user_id)
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    project_store.remove_member(project_id, user_id)
    return {"ok": True}


def _require_membership(project_store: AbstractProjectStore, project_id: str, user_id: str) -> ProjectMember:
    member = project_store.get_member(project_id, user_id)
    if member is None:
        raise HTTPException(status_code=403, detail="Not a member of this project")
    return member


def _find_user_id_by_email(db, email: str) -> str | None:
    if db is None:
        return None
    docs = db.collection("users").where("email", "==", email).limit(1).get()
    for doc in docs:
        return doc.id
    return None
