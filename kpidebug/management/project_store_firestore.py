import uuid

from google.cloud.firestore import Client as FirestoreClient

from kpidebug.management.types import Project, ProjectMember, Role
from kpidebug.management.project_store import AbstractProjectStore


class FirestoreProjectStore(AbstractProjectStore):
    COLLECTION = "projects"
    MEMBERS_SUBCOLLECTION = "members"

    def __init__(self, db: FirestoreClient):
        self.db = db

    def get(self, project_id: str) -> Project | None:
        doc = self.db.collection(self.COLLECTION).document(project_id).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        return Project(
            id=doc.id,
            name=data.get("name", ""),
            description=data.get("description", ""),
        )

    def create(self, name: str, description: str, creator_id: str, creator_name: str, creator_email: str) -> Project:
        project_id = str(uuid.uuid4())
        self.db.collection(self.COLLECTION).document(project_id).set({
            "name": name,
            "description": description,
        })
        self.add_member(project_id, creator_id, Role.ADMIN, creator_name, creator_email)
        return Project(id=project_id, name=name, description=description)

    def update(self, project_id: str, updates: dict) -> Project:
        doc_ref = self.db.collection(self.COLLECTION).document(project_id)
        doc_ref.update(updates)
        return self.get(project_id)

    def delete(self, project_id: str) -> None:
        members = self.get_members(project_id)
        for member in members:
            self._members_collection(project_id).document(member.user_id).delete()
        self.db.collection(self.COLLECTION).document(project_id).delete()

    def list_for_user(self, user_id: str) -> list[Project]:
        membership_docs = self.db.collection_group(self.MEMBERS_SUBCOLLECTION).where(
            "user_id", "==", user_id
        ).get()

        projects: list[Project] = []
        for doc in membership_docs:
            project_id = doc.reference.parent.parent.id
            project = self.get(project_id)
            if project is not None:
                projects.append(project)
        return projects

    def get_members(self, project_id: str) -> list[ProjectMember]:
        docs = self._members_collection(project_id).get()
        members: list[ProjectMember] = []
        for doc in docs:
            data = doc.to_dict()
            members.append(ProjectMember(
                user_id=data.get("user_id", ""),
                role=Role(data.get("role", Role.READ)),
                user_name=data.get("user_name", ""),
                user_email=data.get("user_email", ""),
            ))
        return members

    def add_member(self, project_id: str, user_id: str, role: Role, user_name: str, user_email: str) -> ProjectMember:
        self._members_collection(project_id).document(user_id).set({
            "user_id": user_id,
            "role": role.value,
            "user_name": user_name,
            "user_email": user_email,
        })
        return ProjectMember(user_id=user_id, role=role, user_name=user_name, user_email=user_email)

    def update_member_role(self, project_id: str, user_id: str, role: Role) -> ProjectMember:
        self._members_collection(project_id).document(user_id).update({"role": role.value})
        return self.get_member(project_id, user_id)

    def remove_member(self, project_id: str, user_id: str) -> None:
        self._members_collection(project_id).document(user_id).delete()

    def get_member(self, project_id: str, user_id: str) -> ProjectMember | None:
        doc = self._members_collection(project_id).document(user_id).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        return ProjectMember(
            user_id=data.get("user_id", ""),
            role=Role(data.get("role", Role.READ)),
            user_name=data.get("user_name", ""),
            user_email=data.get("user_email", ""),
        )

    def _members_collection(self, project_id: str):
        return self.db.collection(self.COLLECTION).document(project_id).collection(self.MEMBERS_SUBCOLLECTION)
