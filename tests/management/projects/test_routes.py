from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from kpidebug.api.auth import get_current_user, require_project_role
from kpidebug.api.stores import get_project_store, get_user_store
from kpidebug.management.types import Project, ProjectMember, Role, User
from kpidebug.api.server import app


def _mock_user() -> User:
    return User(id="user1", name="Alice", email="alice@test.com", avatar_url="")


def _mock_project() -> Project:
    return Project(id="proj1", name="Test Project", description="A test project")


def _mock_project_store() -> MagicMock:
    store = MagicMock()
    store.list_for_user.return_value = [_mock_project()]
    store.create.return_value = _mock_project()
    store.get.return_value = _mock_project()
    store.get_member.return_value = ProjectMember(user_id="user1", role=Role.ADMIN, user_name="Alice", user_email="alice@test.com")
    store.get_members.return_value = [
        ProjectMember(user_id="user1", role=Role.ADMIN, user_name="Alice", user_email="alice@test.com"),
    ]
    store.update.return_value = Project(id="proj1", name="Updated", description="Updated desc")
    store.add_member.return_value = ProjectMember(user_id="user2", role=Role.READ, user_name="Bob", user_email="bob@test.com")
    store.update_member_role.return_value = ProjectMember(user_id="user2", role=Role.EDIT, user_name="Bob", user_email="bob@test.com")
    return store


def _mock_user_store() -> MagicMock:
    store = MagicMock()
    store.get.return_value = User(id="user2", name="Bob", email="bob@test.com", avatar_url="")
    return store


class TestProjectRoutes:
    def setup_method(self):
        self.project_store = _mock_project_store()
        self.user_store = _mock_user_store()
        app.dependency_overrides[get_current_user] = lambda: _mock_user()
        app.dependency_overrides[get_project_store] = lambda: self.project_store
        app.dependency_overrides[get_user_store] = lambda: self.user_store
        # Override the admin role check to always pass
        app.dependency_overrides[require_project_role(Role.ADMIN)] = lambda: ProjectMember(
            user_id="user1", role=Role.ADMIN, user_name="Alice", user_email="alice@test.com"
        )
        self.client = TestClient(app)

    def teardown_method(self):
        app.dependency_overrides.clear()

    def test_list_projects(self):
        response = self.client.get("/api/projects")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "Test Project"

    def test_create_project(self):
        response = self.client.post("/api/projects", json={"name": "New Project", "description": "Desc"})
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Test Project"

    def test_get_project(self):
        response = self.client.get("/api/projects/proj1")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "proj1"

    def test_update_project(self):
        response = self.client.put(
            "/api/projects/proj1",
            json={"name": "Updated"},
            headers={"X-Project-Id": "proj1"},
        )
        assert response.status_code == 200

    def test_delete_project(self):
        response = self.client.delete(
            "/api/projects/proj1",
            headers={"X-Project-Id": "proj1"},
        )
        assert response.status_code == 200
        assert response.json()["ok"] is True

    def test_list_members(self):
        response = self.client.get("/api/projects/proj1/members")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["user_id"] == "user1"

    def test_health(self):
        response = self.client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
