from abc import ABC, abstractmethod

from kpidebug.management.types import User


class AbstractUserStore(ABC):
    @abstractmethod
    def get(self, user_id: str) -> User | None:
        ...

    @abstractmethod
    def create(self, user: User) -> User:
        ...

    @abstractmethod
    def update(self, user_id: str, updates: dict) -> User:
        ...

    @abstractmethod
    def get_or_create(self, user_id: str, email: str | None, name: str | None, avatar_url: str | None) -> User:
        ...
