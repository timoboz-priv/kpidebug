from abc import ABC, abstractmethod

from kpidebug.management.types import Project, ProjectMember, Role


class AbstractProjectStore(ABC):
    @abstractmethod
    def get(self, project_id: str) -> Project | None:
        ...

    @abstractmethod
    def create(self, name: str, description: str, creator_id: str, creator_name: str, creator_email: str) -> Project:
        ...

    @abstractmethod
    def update(self, project_id: str, updates: dict) -> Project:
        ...

    @abstractmethod
    def delete(self, project_id: str) -> None:
        ...

    @abstractmethod
    def list_for_user(self, user_id: str) -> list[Project]:
        ...

    @abstractmethod
    def get_members(self, project_id: str) -> list[ProjectMember]:
        ...

    @abstractmethod
    def add_member(self, project_id: str, user_id: str, role: Role, user_name: str, user_email: str) -> ProjectMember:
        ...

    @abstractmethod
    def update_member_role(self, project_id: str, user_id: str, role: Role) -> ProjectMember:
        ...

    @abstractmethod
    def remove_member(self, project_id: str, user_id: str) -> None:
        ...

    @abstractmethod
    def get_member(self, project_id: str, user_id: str) -> ProjectMember | None:
        ...
