from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from dataclasses import dataclass

from dataclasses_json import dataclass_json

from kpidebug.api.auth import (
    get_current_user,
    require_project_role,
)
from kpidebug.api.stores import (
    get_artifact_store,
    get_dashboard_store,
    get_data_source_store,
    get_metric_store,
    get_project_store,
    get_user_store,
)
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.management.artifact_store import AbstractArtifactStore
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.management.types import (
    AddMemberRequest,
    ArtifactType,
    Project,
    ProjectArtifact,
    ProjectMember,
    Role,
    User,
)
from kpidebug.management.project_store import AbstractProjectStore
from kpidebug.management.user_store import AbstractUserStore
from kpidebug.management.summary_agent import generate_summary

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
    updates: dict = {
        "name": body.name,
        "description": body.description,
    }
    if body.summary is not None:
        updates["summary"] = body.summary
    return project_store.update(project_id, updates)


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
    target_user = user_store.get_by_email(body.email)
    if target_user is None:
        raise HTTPException(status_code=404, detail="No user found with that email. They must sign in at least once first.")
    user_id = target_user.id

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


@router.post("/{project_id}/generate-summary")
def generate_project_summary(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    project_store: AbstractProjectStore = Depends(get_project_store),
    artifact_store: AbstractArtifactStore = Depends(get_artifact_store),
) -> Project:
    try:
        summary = generate_summary(artifact_store, project_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    return project_store.update(project_id, {"summary": summary})


@router.post("/{project_id}/process")
def process_project(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> dict[str, bool]:
    from kpidebug.processor import process_all
    process_all(
        project_id=project_id,
        data_source_store=data_source_store,
        dashboard_store=dashboard_store,
        metric_store=metric_store,
    )
    return {"ok": True}


def _require_membership(project_store: AbstractProjectStore, project_id: str, user_id: str) -> ProjectMember:
    member = project_store.get_member(project_id, user_id)
    if member is None:
        raise HTTPException(status_code=403, detail="Not a member of this project")
    return member


# --- Artifacts ---

@dataclass_json
@dataclass
class CreateUrlArtifactRequest:
    url: str = ""


@router.get("/{project_id}/artifacts")
def list_artifacts(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    artifact_store: AbstractArtifactStore = Depends(get_artifact_store),
) -> list[ProjectArtifact]:
    return artifact_store.list(project_id)


@router.post("/{project_id}/artifacts/url")
def create_url_artifact(
    project_id: str,
    body: CreateUrlArtifactRequest,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    artifact_store: AbstractArtifactStore = Depends(get_artifact_store),
) -> ProjectArtifact:
    if not body.url.strip():
        raise HTTPException(status_code=400, detail="URL is required")
    return artifact_store.create_url(project_id, body.url.strip())


@router.post("/{project_id}/artifacts/file")
async def create_file_artifact(
    project_id: str,
    file: UploadFile = File(...),
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    artifact_store: AbstractArtifactStore = Depends(get_artifact_store),
) -> ProjectArtifact:
    content = await file.read()
    return artifact_store.create_file(
        project_id=project_id,
        file_name=file.filename or "unknown",
        file_size=len(content),
        file_mime_type=file.content_type or "application/octet-stream",
        file_content=content,
    )


@router.delete("/{project_id}/artifacts/{artifact_id}")
def delete_artifact(
    project_id: str,
    artifact_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    artifact_store: AbstractArtifactStore = Depends(get_artifact_store),
) -> dict[str, bool]:
    artifact_store.delete(project_id, artifact_id)
    return {"ok": True}


