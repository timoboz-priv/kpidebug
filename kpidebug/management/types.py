from dataclasses import dataclass
from enum import Enum

from dataclasses_json import dataclass_json


class Role(str, Enum):
    READ = "read"
    EDIT = "edit"
    ADMIN = "admin"


@dataclass_json
@dataclass
class User:
    id: str = ""
    name: str = ""
    email: str = ""
    avatar_url: str = ""


@dataclass_json
@dataclass
class Project:
    id: str = ""
    name: str = ""
    description: str = ""
    summary: str | None = None


@dataclass_json
@dataclass
class ProjectMember:
    user_id: str = ""
    role: Role = Role.READ
    user_name: str = ""
    user_email: str = ""


@dataclass_json
@dataclass
class AddMemberRequest:
    email: str = ""
    role: Role = Role.READ


class ArtifactType(str, Enum):
    URL = "url"
    FILE = "file"


@dataclass_json
@dataclass
class ProjectArtifact:
    id: str = ""
    project_id: str = ""
    type: ArtifactType = ArtifactType.URL
    name: str = ""
    value: str = ""
    file_name: str = ""
    file_size: int = 0
    file_mime_type: str = ""
    created_at: str = ""
