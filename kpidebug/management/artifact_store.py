from abc import ABC, abstractmethod

from kpidebug.management.types import ProjectArtifact


class AbstractArtifactStore(ABC):
    @abstractmethod
    def create_url(self, project_id: str, url: str) -> ProjectArtifact:
        ...

    @abstractmethod
    def create_file(
        self,
        project_id: str,
        file_name: str,
        file_size: int,
        file_mime_type: str,
        file_content: bytes,
    ) -> ProjectArtifact:
        ...

    @abstractmethod
    def list(self, project_id: str) -> list[ProjectArtifact]:
        ...

    @abstractmethod
    def get_file_content(self, project_id: str, artifact_id: str) -> bytes | None:
        ...

    @abstractmethod
    def delete(self, project_id: str, artifact_id: str) -> None:
        ...
