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
