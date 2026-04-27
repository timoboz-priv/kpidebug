from unittest.mock import MagicMock, patch

from kpidebug.management.summary_agent import (
    _extract_text_from_html,
    _build_artifact_list_message,
    read_file_content,
)
from kpidebug.management.types import ArtifactType, ProjectArtifact


class TestExtractTextFromHtml:
    def test_strips_html_tags(self):
        html = "<html><body><p>Hello <b>world</b></p></body></html>"
        result = _extract_text_from_html(html)
        assert "Hello" in result
        assert "world" in result
        assert "<" not in result

    def test_removes_script_and_style(self):
        html = "<html><script>alert('x')</script><style>.x{}</style><p>Content</p></html>"
        result = _extract_text_from_html(html)
        assert "alert" not in result
        assert "Content" in result

    def test_truncates_long_content(self):
        html = "<p>" + "x" * 60000 + "</p>"
        result = _extract_text_from_html(html)
        assert len(result) <= 50000


class TestBuildArtifactListMessage:
    def test_empty_artifacts(self):
        result = _build_artifact_list_message([])
        assert "No artifacts" in result

    def test_url_artifact(self):
        artifacts = [
            ProjectArtifact(
                id="a1", project_id="p1", type=ArtifactType.URL,
                value="https://example.com",
            ),
        ]
        result = _build_artifact_list_message(artifacts)
        assert "https://example.com" in result
        assert "read_url_content" in result

    def test_file_artifact(self):
        artifacts = [
            ProjectArtifact(
                id="a2", project_id="p1", type=ArtifactType.FILE,
                file_name="report.pdf", file_mime_type="application/pdf",
                file_size=1024,
            ),
        ]
        result = _build_artifact_list_message(artifacts)
        assert "report.pdf" in result
        assert "read_file_content" in result
        assert "a2" in result


class TestReadFileContent:
    def test_plain_text_file(self):
        import kpidebug.management.summary_agent as agent_module

        artifact = ProjectArtifact(
            id="a1", project_id="p1", type=ArtifactType.FILE,
            file_name="notes.txt", file_mime_type="text/plain", file_size=5,
        )
        mock_store = MagicMock()
        mock_store.get_file_content.return_value = b"Hello world"

        agent_module._artifact_store = mock_store
        agent_module._artifacts = [artifact]

        result = read_file_content("a1")
        assert result == "Hello world"

        agent_module._artifact_store = None
        agent_module._artifacts = []

    def test_artifact_not_found(self):
        import kpidebug.management.summary_agent as agent_module

        agent_module._artifact_store = MagicMock()
        agent_module._artifacts = []

        result = read_file_content("missing")
        assert "not found" in result

        agent_module._artifact_store = None

    def test_store_not_available(self):
        import kpidebug.management.summary_agent as agent_module

        agent_module._artifact_store = None
        agent_module._artifacts = []

        result = read_file_content("a1")
        assert "not available" in result
